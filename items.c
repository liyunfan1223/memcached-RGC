/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memhc.h"
#include "bipbuffer.h"
#include "hash.h"
#include "slab_automove.h"
#include "storage.h"
#include <bits/pthreadtypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#ifdef EXTSTORE
#include "slab_automove_extstore.h"
#endif
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <sys/param.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>
#include <poll.h>

/* Forward Declarations */
static void item_link_q(item *it, uint32_t hv);
static void item_unlink_q(item *it, uint32_t hv);

static unsigned int lru_type_map[4] = {HOT_LRU, WARM_LRU, COLD_LRU, TEMP_LRU};

#define LARGEST_ID POWER_LARGEST

typedef struct {
    uint64_t evicted;
    uint64_t evicted_nonzero;
    uint64_t reclaimed;
    uint64_t outofmemory;
    uint64_t tailrepairs;
    uint64_t expired_unfetched; /* items reclaimed but never touched */
    uint64_t evicted_unfetched; /* items evicted but never touched */
    uint64_t evicted_active; /* items evicted that should have been shuffled */
    uint64_t crawler_reclaimed;
    uint64_t crawler_items_checked;
    uint64_t lrutail_reflocked;
    uint64_t moves_to_cold;
    uint64_t moves_to_warm;
    uint64_t moves_within_lru;
    uint64_t direct_reclaims;
    uint64_t hits_to_hot;
    uint64_t hits_to_warm;
    uint64_t hits_to_cold;
    uint64_t hits_to_temp;
    uint64_t mem_requested;
    uint32_t est_misses;
    uint32_t hits;
    uint32_t sim_hits;
    uint32_t sim_est_misses;
    uint32_t ghost_size;
    uint32_t sim_ghost_size;
    rel_time_t evicted_time;
} itemstats_t;

static item *heads[LARGEST_ID];
static item *tails[LARGEST_ID];
static itemstats_t itemstats[LARGEST_ID];
// pthread_mutex_t itemstats_mutex;

static unsigned int sizes[LARGEST_ID];
static uint64_t sizes_bytes[LARGEST_ID];
static unsigned int *stats_sizes_hist = NULL;
static uint64_t stats_sizes_cas_min = 0;
static int stats_sizes_buckets = 0;
static uint64_t cas_id = 0;

#ifdef WITH_HILL
static ghost_item* ghost_item_buffer_head;
static pthread_mutex_t ghost_item_buffer_lock;

typedef struct _ghost_cache_t {
    uint32_t a;
} ghost_cache_t;

static hill_t     hills[LARGEST_ID];
static hill_sim_t hills_sim[LARGEST_ID];

ghost_item** sim_hashtable;
pthread_mutex_t* sim_hashtable_mutex;

ghost_item** ghost_hashtable;
ghost_item** sim_ghost_hashtable;
pthread_mutex_t* sim_ghost_hashtable_mutex;
pthread_mutex_t* ghost_hashtable_mutex;

uint32_t ghost_hash_size = 0;
uint32_t ghost_hash_mask = 0;
uint32_t original_decay_interval = 0;
double delta_bound = 0.01;

void hill_init(void)
{
    ghost_hash_size = settings.estimate_item_counts << 2;
    ghost_hash_mask = ghost_hash_size - 1;
    original_decay_interval = settings.estimate_item_counts << 4;
    for (uint32_t i = 0; i < LARGEST_ID; i++) {
        hills[i].access_ts = 0;
        hills[i].decay_interval = original_decay_interval;
        hills[i].next_decay_ts = original_decay_interval;
        hills[i].prev_decay_ts = 0;
        hills[i].update_interval = MIN(10000, settings.estimate_item_counts / EST_SLABS);
        hills[i].top_lru_size = 0;
        hills[i].top_lru_max_size = 0.05 * settings.estimate_item_counts / EST_SLABS;
        memset(hills[i].decay_ts, 0, sizeof(hills[i].decay_ts));
        memset(hills[i].size, 0, sizeof(hills[i].size));
    }
    
    for (uint32_t i = 0; i < LARGEST_ID; i++) {
        hills_sim[i].access_ts = 0;
        hills_sim[i].decay_interval = original_decay_interval / SIMULATOR_DECAY_RATIO;
        hills_sim[i].next_decay_ts = original_decay_interval / SIMULATOR_DECAY_RATIO;
        hills_sim[i].prev_decay_ts = 0;
        hills_sim[i].update_interval = MIN(10000, settings.estimate_item_counts / EST_SLABS);
        hills[i].top_lru_size = 0;
        hills_sim[i].top_lru_max_size = 0.05 * settings.estimate_item_counts / EST_SLABS;
        memset(hills_sim[i].decay_ts, 0, sizeof(hills_sim[i].decay_ts));
        memset(hills_sim[i].size, 0, sizeof(hills_sim[i].size));
    }
    sim_hashtable = (ghost_item**)malloc(ghost_hash_size * sizeof(ghost_item*));
    sim_hashtable_mutex = (pthread_mutex_t*)malloc(ghost_hash_size * sizeof(pthread_mutex_t));
//     ghost_item* sim_hashtable[GHOST_HASHSIZE];
// pthread_mutex_t sim_hashtable_mutex[GHOST_HASHSIZE];

// ghost_item* ghost_hashtable[GHOST_HASHSIZE];
// ghost_item* sim_ghost_hashtable[GHOST_HASHSIZE];
    ghost_hashtable = (ghost_item**)malloc(ghost_hash_size * sizeof(ghost_item*));
    sim_ghost_hashtable = (ghost_item**)malloc(ghost_hash_size * sizeof(ghost_item*));
// pthread_mutex_t sim_ghost_hashtable_mutex[GHOST_HASHSIZE];
// pthread_mutex_t ghost_hashtable_mutex[GHOST_HASHSIZE];
    sim_ghost_hashtable_mutex = (pthread_mutex_t*)malloc(ghost_hash_size * sizeof(pthread_mutex_t));
    ghost_hashtable_mutex = (pthread_mutex_t*)malloc(ghost_hash_size * sizeof(pthread_mutex_t));
    
    // for (int i = 0; i < LARGEST_ID; i++) {
    //     pthread_mutex_init(&itemstats_mutex[i], NULL);
    // }
}

static uint32_t calc_curr_level(hill_t* hill, uint16_t inserted_lv, uint32_t inserted_ts)
{
    uint32_t ret = inserted_lv;
    for (int i = 0; i < HILL_MAX_DECAY_TS; i++) {
        if (hill->decay_ts[i] >= inserted_ts) {
            ret /= 2;
            if (!ret) break;
        } else {
            break;
        }
    }
    return ret;
}

static uint32_t calc_curr_level_sim(hill_sim_t* hill, uint16_t inserted_lv, uint32_t inserted_ts)
{
    uint32_t ret = inserted_lv;
    for (int i = 0; i < HILL_MAX_DECAY_TS; i++) {
        if (hill->decay_ts[i] >= inserted_ts) {
            ret /= 2;
            if (!ret) break;
        } else {
            break;
        }
    }
    return ret;
}


static uint8_t last_access_slabclass_id = 0;
static uint8_t sim_last_access_slabclass_id = 0;

static double double_abs(double a) {
    return a > 0 ? a : -a;
}
#endif

static volatile int do_run_lru_maintainer_thread = 0;
static pthread_mutex_t lru_maintainer_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t cas_id_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t stats_sizes_lock = PTHREAD_MUTEX_INITIALIZER;

void item_stats_reset(void) {
    int i;
    for (i = 0; i < LARGEST_ID; i++) {
        pthread_mutex_lock(&lru_locks[i]);
        memset(&itemstats[i], 0, sizeof(itemstats_t));
        pthread_mutex_unlock(&lru_locks[i]);
    }
}

/* called with class lru lock held */
void do_item_stats_add_crawl(const int i, const uint64_t reclaimed,
        const uint64_t unfetched, const uint64_t checked) {
    itemstats[i].crawler_reclaimed += reclaimed;
    itemstats[i].expired_unfetched += unfetched;
    itemstats[i].crawler_items_checked += checked;
}

typedef struct _lru_bump_buf {
    struct _lru_bump_buf *prev;
    struct _lru_bump_buf *next;
    pthread_mutex_t mutex;
    bipbuf_t *buf;
    uint64_t dropped;
} lru_bump_buf;

typedef struct {
    item *it;
    uint32_t hv;
} lru_bump_entry;

static lru_bump_buf *bump_buf_head = NULL;
static lru_bump_buf *bump_buf_tail = NULL;
static pthread_mutex_t bump_buf_lock = PTHREAD_MUTEX_INITIALIZER;
/* TODO: tunable? Need bench results */
#define LRU_BUMP_BUF_SIZE 8192

static bool lru_bump_async(lru_bump_buf *b, item *it, uint32_t hv);
static uint64_t lru_total_bumps_dropped(void);

/* Get the next CAS id for a new item. */
/* TODO: refactor some atomics for this. */
uint64_t get_cas_id(void) {
    pthread_mutex_lock(&cas_id_lock);
    uint64_t next_id = ++cas_id;
    pthread_mutex_unlock(&cas_id_lock);
    return next_id;
}

void set_cas_id(uint64_t new_cas) {
    pthread_mutex_lock(&cas_id_lock);
    cas_id = new_cas;
    pthread_mutex_unlock(&cas_id_lock);
}

int item_is_flushed(item *it) {
    rel_time_t oldest_live = settings.oldest_live;
    uint64_t cas = ITEM_get_cas(it);
    uint64_t oldest_cas = settings.oldest_cas;
    if (oldest_live == 0 || oldest_live > current_time)
        return 0;
    if ((it->time <= oldest_live)
            || (oldest_cas != 0 && cas != 0 && cas < oldest_cas)) {
        return 1;
    }
    return 0;
}

/* must be locked before call */
unsigned int do_get_lru_size(uint32_t id) {
    return sizes[id];
}

/* Enable this for reference-count debugging. */
#if 0
# define DEBUG_REFCNT(it,op) \
                fprintf(stderr, "item %x refcnt(%c) %d %c%c%c\n", \
                        it, op, it->refcount, \
                        (it->it_flags & ITEM_LINKED) ? 'L' : ' ', \
                        (it->it_flags & ITEM_SLABBED) ? 'S' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif

/**
 * Generates the variable-sized part of the header for an object.
 *
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const unsigned int flags, const int nbytes,
                     char *suffix, uint8_t *nsuffix) {
    if (flags == 0) {
        *nsuffix = 0;
    } else {
        *nsuffix = sizeof(flags);
    }
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

item *do_item_alloc_pull(const size_t ntotal, const unsigned int id) {
    item *it = NULL;
    int i;
    /* If no memory is available, attempt a direct LRU juggle/eviction */
    /* This is a race in order to simplify lru_pull_tail; in cases where
     * locked items are on the tail, you want them to fall out and cause
     * occasional OOM's, rather than internally work around them.
     * This also gives one fewer code path for slab alloc/free
     */
    for (i = 0; i < 10; i++) {
        /* Try to reclaim memory first */
        if (!settings.lru_segmented) {
            lru_pull_tail(id, COLD_LRU, 0, 0, 0, NULL);
        }
        it = slabs_alloc(ntotal, id, 0);

        if (it == NULL) {
            // We send '0' in for "total_bytes" as this routine is always
            // pulling to evict, or forcing HOT -> COLD migration.
            // As of this writing, total_bytes isn't at all used with COLD_LRU.
            if (lru_pull_tail(id, COLD_LRU, 0, LRU_PULL_EVICT, 0, NULL) <= 0) {
                if (settings.lru_segmented) {
                    lru_pull_tail(id, HOT_LRU, 0, 0, 0, NULL);
                } else {
                    break;
                }
            }
        } else {
            break;
        }
    }
    if (i > 0) {
        pthread_mutex_lock(&lru_locks[id]);
        itemstats[id].direct_reclaims += i;
        pthread_mutex_unlock(&lru_locks[id]);
    }

    return it;
}

/* Chain another chunk onto this chunk. */
/* slab mover: if it finds a chunk without ITEM_CHUNK flag, and no ITEM_LINKED
 * flag, it counts as busy and skips.
 * I think it might still not be safe to do linking outside of the slab lock
 */
item_chunk *do_item_alloc_chunk(item_chunk *ch, const size_t bytes_remain) {
    // TODO: Should be a cleaner way of finding real size with slabber calls
    size_t size = bytes_remain + sizeof(item_chunk);
    if (size > settings.slab_chunk_size_max)
        size = settings.slab_chunk_size_max;
    unsigned int id = slabs_clsid(size);

    item_chunk *nch = (item_chunk *) do_item_alloc_pull(size, id);
    if (nch == NULL) {
        // The final chunk in a large item will attempt to be a more
        // appropriately sized chunk to minimize memory overhead. However, if
        // there's no memory available in the lower slab classes we fail the
        // SET. In these cases as a fallback we ensure we attempt to evict a
        // max-size item and reuse a large chunk.
        if (size == settings.slab_chunk_size_max) {
            return NULL;
        } else {
            size = settings.slab_chunk_size_max;
            id = slabs_clsid(size);
            nch = (item_chunk *) do_item_alloc_pull(size, id);

            if (nch == NULL)
                return NULL;
        }
    }

    // link in.
    // ITEM_CHUNK[ED] bits need to be protected by the slabs lock.
    slabs_mlock();
    nch->head = ch->head;
    ch->next = nch;
    nch->prev = ch;
    nch->next = 0;
    nch->used = 0;
    nch->slabs_clsid = id;
    nch->size = size - sizeof(item_chunk);
    nch->it_flags |= ITEM_CHUNK;
    slabs_munlock();
    return nch;
}

item *do_item_alloc(const char *key, const size_t nkey, const unsigned int flags,
                    const rel_time_t exptime, const int nbytes) {
    uint8_t nsuffix;
    item *it = NULL;
    char suffix[40];
    // Avoid potential underflows.
    if (nbytes < 2)
        return 0;

    size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);
    if (settings.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    unsigned int id = slabs_clsid(ntotal);
    unsigned int hdr_id = 0;
    if (id == 0)
        return 0;

    /* This is a large item. Allocate a header object now, lazily allocate
     *  chunks while reading the upload.
     */
    if (ntotal > settings.slab_chunk_size_max) {
        /* We still link this item into the LRU for the larger slab class, but
         * we're pulling a header from an entirely different slab class. The
         * free routines handle large items specifically.
         */
        int htotal = nkey + 1 + nsuffix + sizeof(item) + sizeof(item_chunk);
        if (settings.use_cas) {
            htotal += sizeof(uint64_t);
        }
#ifdef NEED_ALIGN
        // header chunk needs to be padded on some systems
        int remain = htotal % 8;
        if (remain != 0) {
            htotal += 8 - remain;
        }
#endif
        hdr_id = slabs_clsid(htotal);
        it = do_item_alloc_pull(htotal, hdr_id);
        /* setting ITEM_CHUNKED is fine here because we aren't LINKED yet. */
        if (it != NULL)
            it->it_flags |= ITEM_CHUNKED;
    } else {
        it = do_item_alloc_pull(ntotal, id);
    }

    if (it == NULL) {
        pthread_mutex_lock(&lru_locks[id]);
        itemstats[id].outofmemory++;
        pthread_mutex_unlock(&lru_locks[id]);
        return NULL;
    }

    assert(it->it_flags == 0 || it->it_flags == ITEM_CHUNKED);
    //assert(it != heads[id]);

    /* Refcount is seeded to 1 by slabs_alloc() */
    it->next = it->prev = 0;

    /* Items are initially loaded into the HOT_LRU. This is '0' but I want at
     * least a note here. Compiler (hopefully?) optimizes this out.
     */
    if (settings.temp_lru &&
            exptime - current_time <= settings.temporary_ttl) {
        id |= TEMP_LRU;
    } else if (settings.lru_segmented) {
        id |= HOT_LRU;
    } else {
        /* There is only COLD in compat-mode */
        id |= COLD_LRU;
    }
    it->slabs_clsid = id;

    DEBUG_REFCNT(it, '*');
    it->it_flags |= settings.use_cas ? ITEM_CAS : 0;
    it->it_flags |= nsuffix != 0 ? ITEM_CFLAGS : 0;
    it->nkey = nkey;
    it->nbytes = nbytes;
    memcpy(ITEM_key(it), key, nkey);
    it->exptime = exptime;
    if (nsuffix > 0) {
        memcpy(ITEM_suffix(it), &flags, sizeof(flags));
    }

    /* Initialize internal chunk. */
    if (it->it_flags & ITEM_CHUNKED) {
        item_chunk *chunk = (item_chunk *) ITEM_schunk(it);

        chunk->next = 0;
        chunk->prev = 0;
        chunk->used = 0;
        chunk->size = 0;
        chunk->head = it;
        chunk->orig_clsid = hdr_id;
    }
    it->h_next = 0;

    return it;
}

void item_free(item *it) {
    size_t ntotal = ITEM_ntotal(it);
    unsigned int clsid;
    assert((it->it_flags & ITEM_LINKED) == 0);
    assert(it != heads[it->slabs_clsid]);
    assert(it != tails[it->slabs_clsid]);
    assert(it->refcount == 0);

    /* so slab size changer can tell later if item is already free or not */
    clsid = ITEM_clsid(it);
    DEBUG_REFCNT(it, 'F');
    slabs_free(it, ntotal, clsid);
}

/**
 * Returns true if an item will fit in the cache (its size does not exceed
 * the maximum for a cache entry.)
 */
bool item_size_ok(const size_t nkey, const int flags, const int nbytes) {
    char prefix[40];
    uint8_t nsuffix;
    if (nbytes < 2)
        return false;

    size_t ntotal = item_make_header(nkey + 1, flags, nbytes,
                                     prefix, &nsuffix);
    if (settings.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    return slabs_clsid(ntotal) != 0;
}

/* fixing stats/references during warm start */
void do_item_link_fixup(item *it) {
    item **head, **tail;
    int ntotal = ITEM_ntotal(it);
    uint32_t hv = hash(ITEM_key(it), it->nkey);
    assoc_insert(it, hv);

    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];
    if (it->prev == 0 && *head == 0) *head = it;
    if (it->next == 0 && *tail == 0) *tail = it;
    sizes[it->slabs_clsid]++;
    sizes_bytes[it->slabs_clsid] += ntotal;

    STATS_LOCK();
    stats_state.curr_bytes += ntotal;
    stats_state.curr_items += 1;
    stats.total_items += 1;
    STATS_UNLOCK();

    item_stats_sizes_add(it);

    return;
}

static void do_item_link_q(item *it, uint32_t hv) { /* item is the new head */

#ifdef WITH_HILL
    uint32_t inserted_lv = DEFAULT_INSERTED_LEVEL;
    /* remove gitem from hashtable */
    uint32_t hv2 = hash2(ITEM_key(it), it->nkey);
    last_access_slabclass_id = it->slabs_clsid;
    ghost_item *gitem = find_ghost_item(hv, hv2, false);
    if (gitem) {
        uint8_t gt_id = gitem->slabs_clsid;
        pthread_mutex_lock(&lru_locks[gt_id]);
        hill_t* gt_hill = &hills[gt_id];
        inserted_lv = MIN(
            (gitem ? calc_curr_level(gt_hill, gitem->inserted_lv, gitem->inserted_ts) : 0) + DEFAULT_INSERTED_LEVEL, 
            HILL_MAX_LEVEL - 1);
        /* ghost item */
        if (find_ghost_item(hv, hv2, false)) {
            ghost_item_remove(gitem, hv, hv2, false);
            // if (gt_id != it->slabs_clsid) {
            ghost_item_remove_maintain(gt_hill, gitem, gt_id, false);
            ghost_item_free(gitem);
        }
        pthread_mutex_unlock(&lru_locks[gt_id]);
    }
    pthread_mutex_lock(&lru_locks[it->slabs_clsid]);
#endif
    item **head, **tail;
    assert((it->it_flags & ITEM_SLABBED) == 0);

    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];

    assert(it != *head);
    assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = 0;
    it->next = *head;
    if (it->next) it->next->prev = it;
    *head = it;
    if (*tail == 0) *tail = it;
    sizes[it->slabs_clsid]++;
#ifdef EXTSTORE
    if (it->it_flags & ITEM_HDR) {
        sizes_bytes[it->slabs_clsid] += (ITEM_ntotal(it) - it->nbytes) + sizeof(item_hdr);
    } else {
        sizes_bytes[it->slabs_clsid] += ITEM_ntotal(it);
    }
#else
    sizes_bytes[it->slabs_clsid] += ITEM_ntotal(it);
#endif

#ifdef WITH_HILL
    /* Yunfan */
    hill_t* hill = &hills[it->slabs_clsid];
    hill->access_ts++;
    // hill->interval_hit++;
    
    assert(inserted_lv >= 0 && inserted_lv < HILL_MAX_LEVEL);

    if (hill->top_lru_size >= hill->top_lru_max_size) {
        item* evicted = hill->top_tail;
        uint32_t evicted_lv = MIN(HILL_MAX_LEVEL - 1, evicted->inserted_lv + DEFAULT_INSERTED_LEVEL);
        uint32_t evicted_ts = hill->access_ts;
        evicted->inserted_ts = evicted_ts;
        evicted->inserted_lv = evicted_lv;
        // top-lru pop tail
        hill->top_tail = evicted->mprev;
        if (hill->top_head == evicted) hill->top_head = NULL;
        if (evicted->mprev) evicted->mprev->mnext = NULL;
        hill->top_lru_size -= 1;

        // move to [evicted_lv] multi-level linked list
        hill->size[evicted_lv] += 1;
        assert(hill->size[evicted_lv] <= 2000000000);
        item **mhead, **mtail;
        mhead = &hill->heads[evicted_lv];
        mtail = &hill->tails[evicted_lv];
        assert((*mhead && *mtail) || (*mhead == 0 && *mtail == 0));
        evicted->mprev = 0;
        evicted->mnext = *mhead;
        if (evicted->mnext) evicted->mnext->mprev = evicted;
        *mhead = evicted;
        if (*mtail == 0) *mtail = evicted;
    }
    // assert(inserted_lv < 0);
    it->inserted_lv = inserted_lv;
    it->inserted_ts = TOP_LRU_TS;
    hill->top_lru_size += 1;
    hill->total_size += 1;
    item **mhead, **mtail;
    mhead = &hill->top_head;
    mtail = &hill->top_tail;
    assert((*mhead && *mtail) || (*mhead == 0 && *mtail == 0));
    it->mprev = 0;
    it->mnext = *mhead;
    if (it->mnext) it->mnext->mprev = it;
    *mhead = it;
    if (*mtail == 0) *mtail = it;

    

    // if (hill->decay_interval && hill->access_ts % hill->decay_interval == 0) {
    if (hill->access_ts % hill->update_interval == 0) {
        // pthread_mutex_lock(&itemstats_mutex[it->slabs_clsid]);
        // pthread_mutex_lock(&itemstats_mutex);
        uint32_t interval_hits = itemstats[it->slabs_clsid].hits - hill->last_update_hits;
        uint32_t interval_misses = itemstats[it->slabs_clsid].est_misses - hill->last_update_misses;

        uint32_t interval_hits_sim = itemstats[it->slabs_clsid].sim_hits - hill->last_update_hits_sim;
        uint32_t interval_misses_sim = itemstats[it->slabs_clsid].sim_est_misses - hill->last_update_misses_sim;
    #ifndef NDEBUG
        printf("?%u %u %u %u %u\n", it->slabs_clsid, itemstats[it->slabs_clsid].hits, itemstats[it->slabs_clsid].est_misses,
            itemstats[it->slabs_clsid].sim_hits, itemstats[it->slabs_clsid].sim_est_misses);
    #endif
        /* strange that previous stats info may less then current */
        assert(itemstats[it->slabs_clsid].hits >= hill->last_update_hits);
        assert(itemstats[it->slabs_clsid].est_misses >= hill->last_update_misses);
        assert(itemstats[it->slabs_clsid].sim_hits >= hill->last_update_hits_sim);
        assert(itemstats[it->slabs_clsid].sim_est_misses >= hill->last_update_misses_sim);

        hill->last_update_hits = itemstats[it->slabs_clsid].hits;
        hill->last_update_misses = itemstats[it->slabs_clsid].est_misses;
        hill->last_update_hits_sim = itemstats[it->slabs_clsid].sim_hits;
        hill->last_update_misses_sim = itemstats[it->slabs_clsid].sim_est_misses;
        // pthread_mutex_unlock(&itemstats_mutex);
        // pthread_mutex_unlock(&itemstats_mutex[it->slabs_clsid]);

        double hit_ratio = (double)interval_hits / (interval_hits + interval_misses);
        double hit_ratio_sim = (double)interval_hits_sim / (interval_hits_sim + interval_misses_sim);
#ifndef NDEBUG
        printf("%u %u %u %u %u, %.4f %.4f %lu\n", it->slabs_clsid, interval_hits, interval_misses, interval_hits_sim, interval_misses_sim,
            hit_ratio, hit_ratio_sim, hill->decay_interval);
#endif
        if (hit_ratio != 0 && hit_ratio_sim != 0) {
            if (double_abs(hit_ratio - hit_ratio_sim) >= EPSILON) {
                hill->stable_count = 0;
                if (hit_ratio_sim > hit_ratio) {
                    double delta_ratio = (hit_ratio_sim / hit_ratio - 1);
                    delta_ratio = MIN(delta_bound, delta_ratio);
                    hill->decay_interval /= 1 + delta_ratio * LAMBDA;
                } else {
                    double delta_ratio = (hit_ratio / hit_ratio_sim - 1);
                    delta_ratio = MIN(delta_bound, delta_ratio);
                    hill->decay_interval *= 1 + delta_ratio * LAMBDA;
                }
            } else {
                hill->stable_count++;
                if (hill->stable_count == 5) {
                    double delta_ratio = 0.1;
                    delta_ratio = MIN(delta_bound, delta_ratio);
                    if (hill->decay_interval < original_decay_interval) {
                        hill->decay_interval *= 1 + delta_ratio;
                    } else {
                        hill->decay_interval /= 1 + delta_ratio;
                    }
                    hill->stable_count = 0;
                }
            }
        }
        if (hill->decay_interval < 400) {
            hill->decay_interval = 400;
        }
        if (hill->decay_interval > 1e18) {
            hill->decay_interval  = 1e18;
        }
        hill->next_decay_ts = hill->prev_decay_ts + hill->decay_interval;
        pthread_mutex_lock(&sim_locks[it->slabs_clsid]);
        hill_sim_t* hill_sim = &hills_sim[it->slabs_clsid];
        hill_sim->decay_interval = hill->decay_interval / SIMULATOR_DECAY_RATIO;
        hill_sim->next_decay_ts = hill_sim->prev_decay_ts + hill_sim->decay_interval;
        pthread_mutex_unlock(&sim_locks[it->slabs_clsid]);
        
    }

    if (hill->next_decay_ts <= hill->access_ts) {
        hill->prev_decay_ts = hill->access_ts;
        hill->next_decay_ts = hill->access_ts + hill->decay_interval;
        for (uint32_t i = 1; i < HILL_MAX_LEVEL; i++) {
            if (hill->heads[i] == NULL) {
                continue;
            }
            uint32_t to = i / 2;
            if (hill->heads[to] == NULL) {
                hill->heads[to] = hill->heads[i];
                hill->tails[to] = hill->tails[i];
            } else {
                assert(hill->heads[to] != NULL);
                assert(hill->tails[to] != NULL);
                // hill->heads[to]->mprev = hill->tails[i];
                // hill->heads[to]->mprev->mnext = hill->heads[to];
                // hill->heads[to] = hill->heads[i];
                hill->tails[to]->mnext = hill->heads[i];
                hill->tails[to]->mnext->mprev = hill->tails[to];
                hill->tails[to] = hill->tails[i];
            }
            hill->heads[i] = NULL;
            hill->tails[i] = NULL;
            hill->size[to] += hill->size[i];
            hill->size[i] = 0;
            assert(hill->size[to] <= 2000000000);
            assert(hill->size[i] <= 2000000000);
        }
        for (uint32_t i = HILL_MAX_DECAY_TS - 1; i >= 1; i--) {
            hill->decay_ts[i] = hill->decay_ts[i - 1];
        }
        hill->decay_ts[0] = hill->access_ts;
    }
    pthread_mutex_unlock(&lru_locks[it->slabs_clsid]);
#endif
    return;
}

#ifdef WITH_HILL
static void item_link_q(item *it, uint32_t hv) {
    // pthread_mutex_lock(&lru_locks[it->slabs_clsid]);
    do_item_link_q(it, hv);
    // pthread_mutex_unlock(&lru_locks[it->slabs_clsid]);
}
#else
static void item_link_q(item *it, uint32_t hv) {
    pthread_mutex_lock(&lru_locks[it->slabs_clsid]);
    do_item_link_q(it, hv);
    pthread_mutex_unlock(&lru_locks[it->slabs_clsid]);
}
#endif
static void item_link_q_warm(item *it, uint32_t hv) {
    pthread_mutex_lock(&lru_locks[it->slabs_clsid]);
    do_item_link_q(it, hv);
    itemstats[it->slabs_clsid].moves_to_warm++;
    pthread_mutex_unlock(&lru_locks[it->slabs_clsid]);
}

static void do_item_unlink_q(item *it, uint32_t hv) {
    item **head, **tail;
    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];

    if (*head == it) {
        assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        assert(it->next == 0);
        *tail = it->prev;
    }
    assert(it->next != it);
    assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    sizes[it->slabs_clsid]--;
#ifdef EXTSTORE
    if (it->it_flags & ITEM_HDR) {
        sizes_bytes[it->slabs_clsid] -= (ITEM_ntotal(it) - it->nbytes) + sizeof(item_hdr);
    } else {
        sizes_bytes[it->slabs_clsid] -= ITEM_ntotal(it);
    }
#else
    sizes_bytes[it->slabs_clsid] -= ITEM_ntotal(it);
#endif

#ifdef WITH_HILL
    hill_t* hill = &hills[it->slabs_clsid];
    item **mhead, **mtail;
    uint32_t inserted_lv;
    bool in_top = false;
    if (it->inserted_ts == TOP_LRU_TS) {
        inserted_lv = it->inserted_lv;
        in_top = true;
        mhead = &hill->top_head;
        mtail = &hill->top_tail;
    } else {
        inserted_lv = calc_curr_level(hill, it->inserted_lv, it->inserted_ts);
        in_top = false;
        mhead = &hill->heads[inserted_lv];
        mtail = &hill->tails[inserted_lv];
    }
    if (*mhead == it) {
        assert(it->mprev == 0);
        *mhead = it->mnext;
    }
    if (*mtail == it) {
        assert(it->mnext == 0);
        *mtail = it->mprev;
    }
    assert(it->mnext != it);
    assert(it->mprev != it);

    if (it->mnext) it->mnext->mprev = it->mprev;
    if (it->mprev) it->mprev->mnext = it->mnext;
    // assert(hill->size[inserted_lv] > 0);
    if (!in_top) {
        hill->size[inserted_lv] -= 1;
    } else {
        hill->top_lru_size -= 1;
    }
    hill->total_size -= 1;
    // assert(hill->size[inserted_lv] <= 2000000000);

    /* ghost item */
    uint32_t hv2 = hash2(ITEM_key(it), it->nkey);
    ghost_item *gitem = ghost_item_alloc();
    gitem->inserted_ts = hill->access_ts;
    gitem->inserted_lv = inserted_lv;
    gitem->hv = hv;
    gitem->hv2 = hv2;
    gitem->slabs_clsid = it->slabs_clsid;
    // assert(gitem->hv != 2217255169);
    ghost_item_insert(gitem, hv, hv2, false);
    ghost_item_insert_maintain(hill, gitem, it->slabs_clsid, false);
#endif
    return;
}

static void item_unlink_q(item *it, uint32_t hv) {
    pthread_mutex_lock(&lru_locks[it->slabs_clsid]);
    do_item_unlink_q(it, hv);
    pthread_mutex_unlock(&lru_locks[it->slabs_clsid]);
}

int do_item_link(item *it, const uint32_t hv) {
    MEMCACHED_ITEM_LINK(ITEM_key(it), it->nkey, it->nbytes);
    assert((it->it_flags & (ITEM_LINKED|ITEM_SLABBED)) == 0);
    it->it_flags |= ITEM_LINKED;
    it->time = current_time;

    STATS_LOCK();
    stats_state.curr_bytes += ITEM_ntotal(it);
    stats_state.curr_items += 1;
    stats.total_items += 1;
    STATS_UNLOCK();

    /* Allocate a new CAS ID on link. */
    ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);
    assoc_insert(it, hv);
    item_link_q(it, hv);
    refcount_incr(it);
    item_stats_sizes_add(it);

    return 1;
}

#ifdef WITH_HILL

void do_item_unlink_q_sim(ghost_item *it) {
    /* Yunfan */
    uint32_t hv = it->hv;
    uint32_t hv2 = it->hv2;

    assert(sim_assoc_find_byhv(it->hv, it->hv2) != NULL);
    assert(find_ghost_item(hv, hv2, true) == NULL);
    hill_sim_t* hill = &hills_sim[it->slabs_clsid];
    ghost_item **ghead, **gtail;
    uint32_t inserted_lv;
    bool in_top = false;
    if (it->inserted_ts == TOP_LRU_TS) {
        inserted_lv = it->inserted_lv;
        in_top = true;
        ghead = &hill->top_head;
        gtail = &hill->top_tail;
    } else {
        inserted_lv = calc_curr_level_sim(hill, it->inserted_lv, it->inserted_ts);
        in_top = false;
        ghead = &hill->heads[inserted_lv];
        gtail = &hill->tails[inserted_lv];
    }
    // assert((*ghead)->inserted_ts != 18839);
    assert(*ghead == NULL || (*ghead)->slabs_clsid != 0);
    assert(*ghead == NULL || (*ghead)->slabs_clsid != 255);
    assert(*gtail == NULL || (*gtail)->slabs_clsid != 0);
    assert((*ghead && *gtail) || (*ghead == 0 && *gtail == 0));
    if (*ghead == it) {
        assert(it->gprev == 0);
        *ghead = it->gnext;
    }
    if (*gtail == it) {
        assert(it->gnext == 0);
        *gtail = it->gprev;
    }
    assert(*ghead == NULL || (*ghead)->slabs_clsid != 0);
    assert(*gtail == NULL || (*gtail)->slabs_clsid != 0);
    assert((*ghead && *gtail) || (*ghead == 0 && *gtail == 0));
    assert(it->gnext != it);
    assert(it->gprev != it);

    if (it->gnext) it->gnext->gprev = it->gprev;
    if (it->gprev) it->gprev->gnext = it->gnext;
    // assert(hill->size[inserted_lv] > 0);
    if (!in_top) {
        hill->size[inserted_lv] -= 1;
        assert(hill->size[inserted_lv] <= 2000000000);
    } else {
        hill->top_lru_size -= 1;
        assert(hill->top_lru_size <= 2000000000);
    }
    hill->total_size -= 1;
    
    sim_assoc_remove(it, hv, hv2);

    /* ghost item */
    ghost_item *gitem = it;
    gitem->inserted_ts = hill->access_ts;
    gitem->inserted_lv = inserted_lv;
    gitem->hv = hv;
    gitem->hv2 = hv2;
    gitem->slabs_clsid = it->slabs_clsid;
    
    ghost_item_insert(gitem, hv, hv2, true);
    ghost_item_insert_maintain((void *)hill, gitem, it->slabs_clsid, true);
    // ghost_item_free(it);
    assert((*ghead && *gtail) || (*ghead == 0 && *gtail == 0));
    return;
}

int do_item_link_sim(ghost_item *git, const uint8_t slabs_clsid) {
    
    
    do_item_link_q_sim(git, slabs_clsid);
    
    return 1;
}

void do_item_link_q_sim(ghost_item* git, uint8_t slabs_clsid)
{
    uint32_t hv = git->hv;
    uint32_t hv2 = git->hv2;
    uint32_t inserted_lv = DEFAULT_INSERTED_LEVEL;
    /* there is a concurrent bug, if multiple thread operator items with the same key */
    ghost_item *gitem = find_ghost_item(hv, hv2, true);
    if (gitem) {
        uint8_t gt_id = gitem->slabs_clsid;
        pthread_mutex_lock(&sim_locks[gt_id]);
        hill_sim_t* gt_hill = &hills_sim[gt_id];
        inserted_lv = MIN(
            (gitem ? calc_curr_level_sim(gt_hill, gitem->inserted_lv, gitem->inserted_ts) : 0) + DEFAULT_INSERTED_LEVEL, 
            HILL_MAX_LEVEL - 1);
        /* ghost item */
        // assert(find_ghost_item(hv, hv2, true));
        if (find_ghost_item(hv, hv2, true)) {
            // pthread_mutex_lock(&sim_locks[gt_id]);
            ghost_item_remove(gitem, hv, hv2, true);
            ghost_item_remove_maintain((void *)gt_hill, gitem, gt_id, true);
            assert(find_ghost_item(hv, hv2, true) == NULL);
            // pthread_mutex_lock(&sim_locks[gt_id]);
            ghost_item_free(gitem);
        }
        pthread_mutex_unlock(&sim_locks[gt_id]);
    }

    pthread_mutex_lock(&sim_locks[slabs_clsid]);
    assert(sim_assoc_find_byhv(git->hv, git->hv2) == NULL);
    // assert(find_ghost_item(hv, hv2, true) == NULL);
    hill_sim_t* hill = &hills_sim[slabs_clsid];
    sim_last_access_slabclass_id = slabs_clsid;
    if (hill->total_size > 0 && hill->total_size >= hills[slabs_clsid].total_size) {
        pull_tail_sim(slabs_clsid);
    }
    hill->access_ts++;
    hill->interval_hit++;
    /* remove gitem from hashtable */
    if (hill->top_lru_size >= hill->top_lru_max_size) {
        ghost_item* evicted = hill->top_tail;
        uint32_t evicted_lv = MIN(HILL_MAX_LEVEL - 1, evicted->inserted_lv + DEFAULT_INSERTED_LEVEL);
        uint32_t evicted_ts = hill->access_ts;
        evicted->inserted_ts = evicted_ts;
        evicted->inserted_lv = evicted_lv;
        // top-lru pop tail
        hill->top_tail = evicted->gprev;
        if (hill->top_head == evicted) hill->top_head = NULL;
        if (evicted->gprev) evicted->gprev->gnext = NULL;
        hill->top_lru_size -= 1;

        // move to [evicted_lv] multi-level linked list
        hill->size[evicted_lv] += 1;
        assert(hill->size[evicted_lv] <= 2000000000);
        ghost_item **ghead, **gtail;
        ghead = &hill->heads[evicted_lv];
        gtail = &hill->tails[evicted_lv];
        assert((*ghead && *gtail) || (*ghead == 0 && *gtail == 0));
        evicted->gprev = 0;
        evicted->gnext = *ghead;
        if (evicted->gnext) evicted->gnext->gprev = evicted;
        *ghead = evicted;
        if (*gtail == 0) *gtail = evicted;
    }
    // assert(inserted_lv < 0);
    git->inserted_lv = inserted_lv;
    git->inserted_ts = TOP_LRU_TS;
    git->slabs_clsid = slabs_clsid;
    
    hill->top_lru_size += 1;
    hill->total_size += 1;
    ghost_item **ghead, **gtail;
    ghead = &hill->top_head;
    gtail = &hill->top_tail;
    assert((*ghead && *gtail) || (*ghead == 0 && *gtail == 0));
    git->gprev = 0;
    git->gnext = *ghead;
    if (git->gnext) git->gnext->gprev = git;
    *ghead = git;
    if (*gtail == 0) *gtail = git;
    assoc_insert_sim(git);
    if (hill->next_decay_ts <= hill->access_ts) {
        hill->prev_decay_ts = hill->access_ts;
        hill->next_decay_ts = hill->access_ts + hill->decay_interval;
        for (uint32_t i = 1; i < HILL_MAX_LEVEL; i++) {
            if (hill->heads[i] == NULL) {
                continue;
            }
            uint32_t to = i / 2;
            if (hill->heads[to] == NULL) {
                hill->heads[to] = hill->heads[i];
                hill->tails[to] = hill->tails[i];
            } else {
                assert(hill->heads[to] != NULL);
                assert(hill->tails[to] != NULL);
                // hill->heads[to]->mprev = hill->tails[i];
                // hill->heads[to]->mprev->mnext = hill->heads[to];
                // hill->heads[to] = hill->heads[i];
                hill->tails[to]->gnext = hill->heads[i];
                hill->tails[to]->gnext->gprev = hill->tails[to];
                hill->tails[to] = hill->tails[i];
            }
            hill->heads[i] = NULL;
            hill->tails[i] = NULL;
            hill->size[to] += hill->size[i];
            hill->size[i] = 0;
            assert(hill->size[to] <= 2000000000);
            assert(hill->size[i] <= 2000000000);
        }
        for (uint32_t i = HILL_MAX_DECAY_TS - 1; i >= 1; i--) {
            hill->decay_ts[i] = hill->decay_ts[i - 1];
        }
        hill->decay_ts[0] = hill->access_ts;
    }
    assert(*ghead == NULL || (*ghead)->slabs_clsid != 0);
    assert(*gtail == NULL || (*gtail)->slabs_clsid != 0);
    assert(git->slabs_clsid != 0);
    assert(*gtail == NULL || (*gtail)->slabs_clsid != 0);
    assert((*ghead && *gtail) || (*ghead == 0 && *gtail == 0));
    pthread_mutex_unlock(&sim_locks[slabs_clsid]);
    return;
}
#endif

void do_item_unlink(item *it, const uint32_t hv) {
    MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
    if ((it->it_flags & ITEM_LINKED) != 0) {
    // assert(it->it_flags & ITEM_LINKED);
        it->it_flags &= ~ITEM_LINKED;
        STATS_LOCK();
        stats_state.curr_bytes -= ITEM_ntotal(it);
        stats_state.curr_items -= 1;
        STATS_UNLOCK();
        item_stats_sizes_remove(it);
        assoc_delete(ITEM_key(it), it->nkey, hv);
        item_unlink_q(it, hv);
        do_item_remove(it);
    }
}

/* FIXME: Is it necessary to keep this copy/pasted code? */
void do_item_unlink_nolock(item *it, const uint32_t hv) {
    MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
    if ((it->it_flags & ITEM_LINKED) != 0) {
        it->it_flags &= ~ITEM_LINKED;
        STATS_LOCK();
        stats_state.curr_bytes -= ITEM_ntotal(it);
        stats_state.curr_items -= 1;
        STATS_UNLOCK();
        item_stats_sizes_remove(it);
        assoc_delete(ITEM_key(it), it->nkey, hv);
        do_item_unlink_q(it, hv);
        do_item_remove(it);
    }
}

void do_item_remove(item *it) {
    MEMCACHED_ITEM_REMOVE(ITEM_key(it), it->nkey, it->nbytes);
    assert((it->it_flags & ITEM_SLABBED) == 0);
    assert(it->refcount > 0);

    if (refcount_decr(it) == 0) {
        item_free(it);
    }
}

/* Bump the last accessed time, or relink if we're in compat mode */
void do_item_update(item *it) {
    MEMCACHED_ITEM_UPDATE(ITEM_key(it), it->nkey, it->nbytes);
    uint32_t hv = hash(ITEM_key(it), it->nkey);

    /* Hits to COLD_LRU immediately move to WARM. */
    if (settings.lru_segmented) {
        assert((it->it_flags & ITEM_SLABBED) == 0);
        if ((it->it_flags & ITEM_LINKED) != 0) {
            if (ITEM_lruid(it) == COLD_LRU && (it->it_flags & ITEM_ACTIVE)) {
                it->time = current_time;
                item_unlink_q(it, hv);
                it->slabs_clsid = ITEM_clsid(it);
                it->slabs_clsid |= WARM_LRU;
                it->it_flags &= ~ITEM_ACTIVE;
                item_link_q_warm(it, hv);
            } else {
                it->time = current_time;
            }
        }
    // } else if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
    } else {
        assert((it->it_flags & ITEM_SLABBED) == 0);

        // if ((it->it_flags & ITEM_LINKED) != 0) {
        assert(it->it_flags & ITEM_LINKED);
            it->time = current_time;
            item_unlink_q(it, hv);
            item_link_q(it, hv);
        // }
    }
}

int do_item_replace(item *it, item *new_it, const uint32_t hv) {
    MEMCACHED_ITEM_REPLACE(ITEM_key(it), it->nkey, it->nbytes,
                           ITEM_key(new_it), new_it->nkey, new_it->nbytes);
    assert((it->it_flags & ITEM_SLABBED) == 0);
    do_item_unlink(it, hv);
#ifdef WITH_HILL
//     /* replace */
    // new_it->inserted_lv = it->inserted_lv;
    // new_it->inserted_ts = it->inserted_ts;
#endif
    return do_item_link(new_it, hv);
}

/*@null@*/
/* This is walking the line of violating lock order, but I think it's safe.
 * If the LRU lock is held, an item in the LRU cannot be wiped and freed.
 * The data could possibly be overwritten, but this is only accessing the
 * headers.
 * It may not be the best idea to leave it like this, but for now it's safe.
 */
char *item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes) {
    unsigned int memlimit = 2 * 1024 * 1024;   /* 2MB max response size */
    char *buffer;
    unsigned int bufcurr;
    item *it;
    unsigned int len;
    unsigned int shown = 0;
    char key_temp[KEY_MAX_LENGTH + 1];
    char temp[512];
    unsigned int id = slabs_clsid;
    id |= COLD_LRU;

    pthread_mutex_lock(&lru_locks[id]);
    it = heads[id];

    buffer = malloc((size_t)memlimit);
    if (buffer == 0) {
        pthread_mutex_unlock(&lru_locks[id]);
        return NULL;
    }
    bufcurr = 0;

    while (it != NULL && (limit == 0 || shown < limit)) {
        assert(it->nkey <= KEY_MAX_LENGTH);
        // protect from printing binary keys.
        if ((it->nbytes == 0 && it->nkey == 0) || (it->it_flags & ITEM_KEY_BINARY)) {
            it = it->next;
            continue;
        }
        /* Copy the key since it may not be null-terminated in the struct */
        strncpy(key_temp, ITEM_key(it), it->nkey);
        key_temp[it->nkey] = 0x00; /* terminate */
        len = snprintf(temp, sizeof(temp), "ITEM %s [%d b; %llu s]\r\n",
                       key_temp, it->nbytes - 2,
                       it->exptime == 0 ? 0 :
                       (unsigned long long)it->exptime + process_started);
        if (bufcurr + len + 6 > memlimit)  /* 6 is END\r\n\0 */
            break;
        memcpy(buffer + bufcurr, temp, len);
        bufcurr += len;
        shown++;
        it = it->next;
    }

    memcpy(buffer + bufcurr, "END\r\n", 6);
    bufcurr += 5;

    *bytes = bufcurr;
    pthread_mutex_unlock(&lru_locks[id]);
    return buffer;
}

/* With refactoring of the various stats code the automover won't need a
 * custom function here.
 */
void fill_item_stats_automove(item_stats_automove *am) {
    int n;
    for (n = 0; n < MAX_NUMBER_OF_SLAB_CLASSES; n++) {
        item_stats_automove *cur = &am[n];

        // outofmemory records into HOT
        int i = n | HOT_LRU;
        pthread_mutex_lock(&lru_locks[i]);
        cur->outofmemory = itemstats[i].outofmemory;
        pthread_mutex_unlock(&lru_locks[i]);

        // evictions and tail age are from COLD
        i = n | COLD_LRU;
        pthread_mutex_lock(&lru_locks[i]);
        cur->evicted = itemstats[i].evicted;
        if (!tails[i]) {
            cur->age = 0;
        } else if (tails[i]->nbytes == 0 && tails[i]->nkey == 0 && tails[i]->it_flags == 1) {
            /* it's a crawler, check previous entry */
            if (tails[i]->prev) {
               cur->age = current_time - tails[i]->prev->time;
            } else {
               cur->age = 0;
            }
        } else {
            cur->age = current_time - tails[i]->time;
        }
        pthread_mutex_unlock(&lru_locks[i]);
     }
}

void item_stats_totals(ADD_STAT add_stats, void *c) {
    itemstats_t totals;
    memset(&totals, 0, sizeof(itemstats_t));
    int n;
    for (n = 0; n < MAX_NUMBER_OF_SLAB_CLASSES; n++) {
        int x;
        int i;
        for (x = 0; x < 4; x++) {
            i = n | lru_type_map[x];
            pthread_mutex_lock(&lru_locks[i]);
            totals.expired_unfetched += itemstats[i].expired_unfetched;
            totals.evicted_unfetched += itemstats[i].evicted_unfetched;
            totals.evicted_active += itemstats[i].evicted_active;
            totals.evicted += itemstats[i].evicted;
            totals.reclaimed += itemstats[i].reclaimed;
            totals.crawler_reclaimed += itemstats[i].crawler_reclaimed;
            totals.crawler_items_checked += itemstats[i].crawler_items_checked;
            totals.lrutail_reflocked += itemstats[i].lrutail_reflocked;
            totals.moves_to_cold += itemstats[i].moves_to_cold;
            totals.moves_to_warm += itemstats[i].moves_to_warm;
            totals.moves_within_lru += itemstats[i].moves_within_lru;
            totals.direct_reclaims += itemstats[i].direct_reclaims;
            pthread_mutex_unlock(&lru_locks[i]);
        }
    }
    APPEND_STAT("expired_unfetched", "%llu",
                (unsigned long long)totals.expired_unfetched);
    APPEND_STAT("evicted_unfetched", "%llu",
                (unsigned long long)totals.evicted_unfetched);
    if (settings.lru_maintainer_thread) {
        APPEND_STAT("evicted_active", "%llu",
                    (unsigned long long)totals.evicted_active);
    }
    APPEND_STAT("evictions", "%llu",
                (unsigned long long)totals.evicted);
    APPEND_STAT("reclaimed", "%llu",
                (unsigned long long)totals.reclaimed);
    APPEND_STAT("crawler_reclaimed", "%llu",
                (unsigned long long)totals.crawler_reclaimed);
    APPEND_STAT("crawler_items_checked", "%llu",
                (unsigned long long)totals.crawler_items_checked);
    APPEND_STAT("lrutail_reflocked", "%llu",
                (unsigned long long)totals.lrutail_reflocked);
    if (settings.lru_maintainer_thread) {
        APPEND_STAT("moves_to_cold", "%llu",
                    (unsigned long long)totals.moves_to_cold);
        APPEND_STAT("moves_to_warm", "%llu",
                    (unsigned long long)totals.moves_to_warm);
        APPEND_STAT("moves_within_lru", "%llu",
                    (unsigned long long)totals.moves_within_lru);
        APPEND_STAT("direct_reclaims", "%llu",
                    (unsigned long long)totals.direct_reclaims);
        APPEND_STAT("lru_bumps_dropped", "%llu",
                    (unsigned long long)lru_total_bumps_dropped());
    }
}

void item_stats(ADD_STAT add_stats, void *c) {
    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);
    itemstats_t totals;
    int n;
    for (n = 0; n < MAX_NUMBER_OF_SLAB_CLASSES; n++) {
        memset(&totals, 0, sizeof(itemstats_t));
        int x;
        int i;
        unsigned int size = 0;
        unsigned int age  = 0;
        unsigned int age_hot = 0;
        unsigned int age_warm = 0;
        unsigned int lru_size_map[4];
        const char *fmt = "items:%d:%s";
        char key_str[STAT_KEY_LEN];
        char val_str[STAT_VAL_LEN];
        int klen = 0, vlen = 0;
        for (x = 0; x < 4; x++) {
            i = n | lru_type_map[x];
            pthread_mutex_lock(&lru_locks[i]);
            totals.evicted += itemstats[i].evicted;
            totals.evicted_nonzero += itemstats[i].evicted_nonzero;
            totals.outofmemory += itemstats[i].outofmemory;
            totals.tailrepairs += itemstats[i].tailrepairs;
            totals.reclaimed += itemstats[i].reclaimed;
            totals.expired_unfetched += itemstats[i].expired_unfetched;
            totals.evicted_unfetched += itemstats[i].evicted_unfetched;
            totals.evicted_active += itemstats[i].evicted_active;
            totals.crawler_reclaimed += itemstats[i].crawler_reclaimed;
            totals.crawler_items_checked += itemstats[i].crawler_items_checked;
            totals.lrutail_reflocked += itemstats[i].lrutail_reflocked;
            totals.moves_to_cold += itemstats[i].moves_to_cold;
            totals.moves_to_warm += itemstats[i].moves_to_warm;
            totals.moves_within_lru += itemstats[i].moves_within_lru;
            totals.direct_reclaims += itemstats[i].direct_reclaims;
            totals.mem_requested += sizes_bytes[i];
            totals.ghost_size += itemstats[i].ghost_size;
            totals.est_misses += itemstats[i].est_misses;
            totals.hits += itemstats[i].hits;
            totals.sim_est_misses += itemstats[i].sim_est_misses;
            totals.sim_hits += itemstats[i].sim_hits;
            size += sizes[i];
            lru_size_map[x] = sizes[i];
            if (lru_type_map[x] == COLD_LRU && tails[i] != NULL) {
                age = current_time - tails[i]->time;
            } else if (lru_type_map[x] == HOT_LRU && tails[i] != NULL) {
                age_hot = current_time - tails[i]->time;
            } else if (lru_type_map[x] == WARM_LRU && tails[i] != NULL) {
                age_warm = current_time - tails[i]->time;
            }
            if (lru_type_map[x] == COLD_LRU)
                totals.evicted_time = itemstats[i].evicted_time;
            switch (lru_type_map[x]) {
                case HOT_LRU:
                    totals.hits_to_hot = thread_stats.lru_hits[i];
                    break;
                case WARM_LRU:
                    totals.hits_to_warm = thread_stats.lru_hits[i];
                    break;
                case COLD_LRU:
                    totals.hits_to_cold = thread_stats.lru_hits[i];
                    break;
                case TEMP_LRU:
                    totals.hits_to_temp = thread_stats.lru_hits[i];
                    break;
            }
            pthread_mutex_unlock(&lru_locks[i]);
        }
        if (size == 0)
            continue;
        APPEND_NUM_FMT_STAT(fmt, n, "number", "%u", size);
        if (settings.lru_maintainer_thread) {
            APPEND_NUM_FMT_STAT(fmt, n, "number_hot", "%u", lru_size_map[0]);
            APPEND_NUM_FMT_STAT(fmt, n, "number_warm", "%u", lru_size_map[1]);
            APPEND_NUM_FMT_STAT(fmt, n, "number_cold", "%u", lru_size_map[2]);
            if (settings.temp_lru) {
                APPEND_NUM_FMT_STAT(fmt, n, "number_temp", "%u", lru_size_map[3]);
            }
            APPEND_NUM_FMT_STAT(fmt, n, "age_hot", "%u", age_hot);
            APPEND_NUM_FMT_STAT(fmt, n, "age_warm", "%u", age_warm);
        }
        APPEND_NUM_FMT_STAT(fmt, n, "age", "%u", age);
        APPEND_NUM_FMT_STAT(fmt, n, "mem_requested", "%llu", (unsigned long long)totals.mem_requested);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted",
                            "%llu", (unsigned long long)totals.evicted);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted_nonzero",
                            "%llu", (unsigned long long)totals.evicted_nonzero);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted_time",
                            "%u", totals.evicted_time);
        APPEND_NUM_FMT_STAT(fmt, n, "outofmemory",
                            "%llu", (unsigned long long)totals.outofmemory);
        APPEND_NUM_FMT_STAT(fmt, n, "tailrepairs",
                            "%llu", (unsigned long long)totals.tailrepairs);
        APPEND_NUM_FMT_STAT(fmt, n, "reclaimed",
                            "%llu", (unsigned long long)totals.reclaimed);
        APPEND_NUM_FMT_STAT(fmt, n, "expired_unfetched",
                            "%llu", (unsigned long long)totals.expired_unfetched);
        APPEND_NUM_FMT_STAT(fmt, n, "evicted_unfetched",
                            "%llu", (unsigned long long)totals.evicted_unfetched);
        if (settings.lru_maintainer_thread) {
            APPEND_NUM_FMT_STAT(fmt, n, "evicted_active",
                                "%llu", (unsigned long long)totals.evicted_active);
        }
        APPEND_NUM_FMT_STAT(fmt, n, "crawler_reclaimed",
                            "%llu", (unsigned long long)totals.crawler_reclaimed);
        APPEND_NUM_FMT_STAT(fmt, n, "crawler_items_checked",
                            "%llu", (unsigned long long)totals.crawler_items_checked);
        APPEND_NUM_FMT_STAT(fmt, n, "lrutail_reflocked",
                            "%llu", (unsigned long long)totals.lrutail_reflocked);
        APPEND_NUM_FMT_STAT(fmt, n, "ghost_cache_size",
                            "%u", totals.ghost_size);
        APPEND_NUM_FMT_STAT(fmt, n, "estimate_misses",
                            "%u", totals.est_misses);
        APPEND_NUM_FMT_STAT(fmt, n, "hits",
                            "%u", totals.hits);
        APPEND_NUM_FMT_STAT(fmt, n, "sim_estimate_misses",
                            "%u", totals.sim_est_misses);
        APPEND_NUM_FMT_STAT(fmt, n, "sim_hits",
                            "%u", totals.sim_hits);

        if (settings.lru_maintainer_thread) {
            APPEND_NUM_FMT_STAT(fmt, n, "moves_to_cold",
                                "%llu", (unsigned long long)totals.moves_to_cold);
            APPEND_NUM_FMT_STAT(fmt, n, "moves_to_warm",
                                "%llu", (unsigned long long)totals.moves_to_warm);
            APPEND_NUM_FMT_STAT(fmt, n, "moves_within_lru",
                                "%llu", (unsigned long long)totals.moves_within_lru);
            APPEND_NUM_FMT_STAT(fmt, n, "direct_reclaims",
                                "%llu", (unsigned long long)totals.direct_reclaims);
            APPEND_NUM_FMT_STAT(fmt, n, "hits_to_hot",
                                "%llu", (unsigned long long)totals.hits_to_hot);

            APPEND_NUM_FMT_STAT(fmt, n, "hits_to_warm",
                                "%llu", (unsigned long long)totals.hits_to_warm);

            APPEND_NUM_FMT_STAT(fmt, n, "hits_to_cold",
                                "%llu", (unsigned long long)totals.hits_to_cold);

            APPEND_NUM_FMT_STAT(fmt, n, "hits_to_temp",
                                "%llu", (unsigned long long)totals.hits_to_temp);

        } else {
            APPEND_NUM_FMT_STAT(fmt, n, "hits_to_cold",
                                "%llu", (unsigned long long)totals.hits_to_cold);
        }
    }

    /* getting here means both ascii and binary terminators fit */
    add_stats(NULL, 0, NULL, 0, c);
}

bool item_stats_sizes_status(void) {
    bool ret = false;
    mutex_lock(&stats_sizes_lock);
    if (stats_sizes_hist != NULL)
        ret = true;
    mutex_unlock(&stats_sizes_lock);
    return ret;
}

void item_stats_sizes_init(void) {
    if (stats_sizes_hist != NULL)
        return;
    stats_sizes_buckets = settings.item_size_max / 32 + 1;
    stats_sizes_hist = calloc(stats_sizes_buckets, sizeof(int));
    stats_sizes_cas_min = (settings.use_cas) ? get_cas_id() : 0;
}

void item_stats_sizes_enable(ADD_STAT add_stats, void *c) {
    mutex_lock(&stats_sizes_lock);
    if (!settings.use_cas) {
        APPEND_STAT("sizes_status", "error", "");
        APPEND_STAT("sizes_error", "cas_support_disabled", "");
    } else if (stats_sizes_hist == NULL) {
        item_stats_sizes_init();
        if (stats_sizes_hist != NULL) {
            APPEND_STAT("sizes_status", "enabled", "");
        } else {
            APPEND_STAT("sizes_status", "error", "");
            APPEND_STAT("sizes_error", "no_memory", "");
        }
    } else {
        APPEND_STAT("sizes_status", "enabled", "");
    }
    mutex_unlock(&stats_sizes_lock);
}

void item_stats_sizes_disable(ADD_STAT add_stats, void *c) {
    mutex_lock(&stats_sizes_lock);
    if (stats_sizes_hist != NULL) {
        free(stats_sizes_hist);
        stats_sizes_hist = NULL;
    }
    APPEND_STAT("sizes_status", "disabled", "");
    mutex_unlock(&stats_sizes_lock);
}

void item_stats_sizes_add(item *it) {
    if (stats_sizes_hist == NULL || stats_sizes_cas_min > ITEM_get_cas(it))
        return;
    int ntotal = ITEM_ntotal(it);
    int bucket = ntotal / 32;
    if ((ntotal % 32) != 0) bucket++;
    if (bucket < stats_sizes_buckets) stats_sizes_hist[bucket]++;
}

/* I think there's no way for this to be accurate without using the CAS value.
 * Since items getting their time value bumped will pass this validation.
 */
void item_stats_sizes_remove(item *it) {
    if (stats_sizes_hist == NULL || stats_sizes_cas_min > ITEM_get_cas(it))
        return;
    int ntotal = ITEM_ntotal(it);
    int bucket = ntotal / 32;
    if ((ntotal % 32) != 0) bucket++;
    if (bucket < stats_sizes_buckets) stats_sizes_hist[bucket]--;
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
/* Locks are correct based on a technicality. Holds LRU lock while doing the
 * work, so items can't go invalid, and it's only looking at header sizes
 * which don't change.
 */
void item_stats_sizes(ADD_STAT add_stats, void *c) {
    mutex_lock(&stats_sizes_lock);

    if (stats_sizes_hist != NULL) {
        int i;
        for (i = 0; i < stats_sizes_buckets; i++) {
            if (stats_sizes_hist[i] != 0) {
                char key[12];
                snprintf(key, sizeof(key), "%d", i * 32);
                APPEND_STAT(key, "%u", stats_sizes_hist[i]);
            }
        }
    } else {
        APPEND_STAT("sizes_status", "disabled", "");
    }

    add_stats(NULL, 0, NULL, 0, c);
    mutex_unlock(&stats_sizes_lock);
}

/** wrapper around assoc_find which does the lazy expiration logic */
item *do_item_get(const char *key, const size_t nkey, const uint32_t hv, LIBEVENT_THREAD *t, const bool do_update) {
    item *it = assoc_find(key, nkey, hv);
    #ifdef WITH_HILL
    if (do_update) {
        ghost_item* git = sim_assoc_find(key, nkey, hv);
        if (git == NULL) {
            int this_slabclass_id = sim_last_access_slabclass_id;
            if (this_slabclass_id) {
                // assert(this_slabclass_id >= 0 && this_slabclass_id <= LARGEST_ID - 1);
                // pthread_mutex_lock(&itemstats_mutex);
                // uint32_t rec = itemstats[this_slabclass_id].sim_est_misses;
                // assert(rec == itemstats[this_slabclass_id].sim_est_misses);
                itemstats[this_slabclass_id].sim_est_misses += 1;
                // assert(rec + 1 == itemstats[this_slabclass_id].sim_est_misses);
                // pthread_mutex_unlock(&itemstats_mutex);
            }
        } else {
            // pthread_mutex_lock(&itemstats_mutex[git->slabs_clsid]);
            itemstats[git->slabs_clsid].sim_hits += 1;
            // pthread_mutex_unlock(&itemstats_mutex[git->slabs_clsid]);
            /// TODO: do_item_bump_sim
            ghost_item* old_git = sim_assoc_find(key, nkey, hv);
            if (old_git) {
                pthread_mutex_lock(&sim_locks[old_git->slabs_clsid]);
                // double check
                if (sim_assoc_find(key, nkey, hv)) {
                    do_item_unlink_q_sim(old_git);
                }
                pthread_mutex_unlock(&sim_locks[old_git->slabs_clsid]);
            }
            // } else {
            ghost_item* git2 = ghost_item_alloc();
            git2->hv = hv;
            git2->hv2 = hash2(key, nkey);
            git2->slabs_clsid = git->slabs_clsid;
            do_item_link_sim(git2, git->slabs_clsid);
        }
    }
    if (do_update) {
        if (it == NULL) {
            int this_slabclass_id = last_access_slabclass_id;
            if (this_slabclass_id) {
                // assert(this_slabclass_id >= 0 && this_slabclass_id <= LARGEST_ID - 1);
                // // pthread_mutex_lock(&itemstats_mutex[last_access_slabclass_id]);
                // pthread_mutex_lock(&itemstats_mutex);
                // uint32_t rec = itemstats[this_slabclass_id].est_misses ;
                // assert(rec == itemstats[this_slabclass_id].est_misses );
                itemstats[this_slabclass_id].est_misses += 1;
                // assert(rec + 1 == itemstats[this_slabclass_id].est_misses );
                // pthread_mutex_unlock(&itemstats_mutex);
                // pthread_mutex_unlock(&itemstats_mutex[last_access_slabclass_id]);
            }
        } else {
            itemstats[it->slabs_clsid].hits += 1;
        }
    }
    #endif
    if (it != NULL) {
        refcount_incr(it);
        /* Optimization for slab reassignment. prevents popular items from
         * jamming in busy wait. Can only do this here to satisfy lock order
         * of item_lock, slabs_lock. */
        /* This was made unsafe by removal of the cache_lock:
         * slab_rebalance_signal and slab_rebal.* are modified in a separate
         * thread under slabs_lock. If slab_rebalance_signal = 1, slab_start =
         * NULL (0), but slab_end is still equal to some value, this would end
         * up unlinking every item fetched.
         * This is either an acceptable loss, or if slab_rebalance_signal is
         * true, slab_start/slab_end should be put behind the slabs_lock.
         * Which would cause a huge potential slowdown.
         * Could also use a specific lock for slab_rebal.* and
         * slab_rebalance_signal (shorter lock?)
         */
        /*if (slab_rebalance_signal &&
            ((void *)it >= slab_rebal.slab_start && (void *)it < slab_rebal.slab_end)) {
            do_item_unlink(it, hv);
            do_item_remove(it);
            it = NULL;
        }*/
    }
    int was_found = 0;

    if (settings.verbose > 2) {
        int ii;
        if (it == NULL) {
            fprintf(stderr, "> NOT FOUND ");
        } else if (was_found) {
            fprintf(stderr, "> FOUND KEY ");
        }
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
    }

    if (it != NULL) {
        was_found = 1;
        if (item_is_flushed(it)) {
            do_item_unlink(it, hv);
            STORAGE_delete(t->storage, it);
            do_item_remove(it);
            it = NULL;
            pthread_mutex_lock(&t->stats.mutex);
            t->stats.get_flushed++;
            pthread_mutex_unlock(&t->stats.mutex);
            if (settings.verbose > 2) {
                fprintf(stderr, " -nuked by flush");
            }
            was_found = 2;
        } else if (it->exptime != 0 && it->exptime <= current_time) {
            do_item_unlink(it, hv);
            STORAGE_delete(t->storage, it);
            do_item_remove(it);
            it = NULL;
            pthread_mutex_lock(&t->stats.mutex);
            t->stats.get_expired++;
            pthread_mutex_unlock(&t->stats.mutex);
            if (settings.verbose > 2) {
                fprintf(stderr, " -nuked by expire");
            }
            was_found = 3;
        } else {
            if (do_update) {
                do_item_bump(t, it, hv);
            }
            DEBUG_REFCNT(it, '+');
        }
    }

    if (settings.verbose > 2)
        fprintf(stderr, "\n");
    /* For now this is in addition to the above verbose logging. */
    LOGGER_LOG(t->l, LOG_FETCHERS, LOGGER_ITEM_GET, NULL, was_found, key,
               nkey, (it) ? it->nbytes : 0, (it) ? ITEM_clsid(it) : 0, t->cur_sfd);

    return it;
}

// Requires lock held for item.
// Split out of do_item_get() to allow mget functions to look through header
// data before losing state modified via the bump function.
void do_item_bump(LIBEVENT_THREAD *t, item *it, const uint32_t hv) {
    /* We update the hit markers only during fetches.
     * An item needs to be hit twice overall to be considered
     * ACTIVE, but only needs a single hit to maintain activity
     * afterward.
     * FETCHED tells if an item has ever been active.
     */
    if (settings.lru_segmented) {
        if ((it->it_flags & ITEM_ACTIVE) == 0) {
            if ((it->it_flags & ITEM_FETCHED) == 0) {
                it->it_flags |= ITEM_FETCHED;
            } else {
                it->it_flags |= ITEM_ACTIVE;
                if (ITEM_lruid(it) != COLD_LRU) {
                    it->time = current_time; // only need to bump time.
                } else if (!lru_bump_async(t->lru_bump_buf, it, hv)) {
                    // add flag before async bump to avoid race.
                    it->it_flags &= ~ITEM_ACTIVE;
                }
            }
        }
    } else {
        it->it_flags |= ITEM_FETCHED;
        do_item_update(it);
    }
}

item *do_item_touch(const char *key, size_t nkey, uint32_t exptime,
                    const uint32_t hv, LIBEVENT_THREAD *t) {
    item *it = do_item_get(key, nkey, hv, t, DO_UPDATE);
    if (it != NULL) {
        it->exptime = exptime;
    }
    return it;
}

/*** LRU MAINTENANCE THREAD ***/

/* Returns number of items remove, expired, or evicted.
 * Callable from worker threads or the LRU maintainer thread */
int lru_pull_tail(const int orig_id, const int cur_lru,
        const uint64_t total_bytes, const uint8_t flags, const rel_time_t max_age,
        struct lru_pull_tail_return *ret_it) {
    item *it = NULL;
    uint32_t it_hv=0;
    int id = orig_id;
    int removed = 0;
    if (id == 0)
        return 0;

    int tries = 1;
    // int tries = 1;
    item *search;
    item *next_it;
    void *hold_lock = NULL;
    unsigned int move_to_lru = 0;
    uint64_t limit = 0;

    id |= cur_lru;
    // assert(id == 129);
    pthread_mutex_lock(&lru_locks[id]);
#ifdef WITH_HILL
    hill_t* hill = &hills[id];
    uint32_t lowest = 0;
    for (; lowest < HILL_MAX_LEVEL; lowest++) {
        if (hill->heads[lowest] != NULL) {
            break;
        }
    }
    if (lowest < HILL_MAX_LEVEL)
        // search = hill->tails[lowest];
        search = hill->heads[lowest];
    else
        search = NULL;
    assert(search || !(flags & LRU_PULL_EVICT));
#else
    search = tails[id];
#endif

    /* We walk up *only* for locked items, and if bottom is expired. */
    for (; tries > 0 && search != NULL; tries--, search=next_it) {
        /* we might relink search mid-loop, so search->prev isn't reliable */
#ifdef WITH_HILL
        next_it = search->mnext;
#else
        next_it = search->prev;
#endif
        if (search->nbytes == 0 && search->nkey == 0 && search->it_flags == 1) {
            /* We are a crawler, ignore it. */
            if (flags & LRU_PULL_CRAWL_BLOCKS) {
                pthread_mutex_unlock(&lru_locks[id]);
                return 0;
            }
            tries++;
            continue;
        }
        uint32_t hv = hash(ITEM_key(search), search->nkey);
        /* Attempt to hash item lock the "search" item. If locked, no
         * other callers can incr the refcount. Also skip ourselves. */
        if ((hold_lock = item_trylock(hv)) == NULL)
            continue;
        /* Now see if the item is refcount locked */
        if (refcount_incr(search) != 2) {
            /* Note pathological case with ref'ed items in tail.
             * Can still unlink the item, but it won't be reusable yet */
            itemstats[id].lrutail_reflocked++;
            /* In case of refcount leaks, enable for quick workaround. */
            /* WARNING: This can cause terrible corruption */
            if (settings.tail_repair_time &&
                    search->time + settings.tail_repair_time < current_time) {
                itemstats[id].tailrepairs++;
                search->refcount = 1;
                /* This will call item_remove -> item_free since refcnt is 1 */
                STORAGE_delete(ext_storage, search);
                do_item_unlink_nolock(search, hv);
                item_trylock_unlock(hold_lock);
                continue;
            }
        }

        /* Expired or flushed */
        if ((search->exptime != 0 && search->exptime < current_time)
            || item_is_flushed(search)) {
            itemstats[id].reclaimed++;
            if ((search->it_flags & ITEM_FETCHED) == 0) {
                itemstats[id].expired_unfetched++;
            }
            /* refcnt 2 -> 1 */
            do_item_unlink_nolock(search, hv);
            STORAGE_delete(ext_storage, search);
            /* refcnt 1 -> 0 -> item_free */
            do_item_remove(search);
            item_trylock_unlock(hold_lock);
            removed++;

            /* If all we're finding are expired, can keep going */
            continue;
        }

        /* If we're HOT_LRU or WARM_LRU and over size limit, send to COLD_LRU.
         * If we're COLD_LRU, send to WARM_LRU unless we need to evict
         */
        switch (cur_lru) {
            case HOT_LRU:
                limit = total_bytes * settings.hot_lru_pct / 100;
            case WARM_LRU:
                if (limit == 0)
                    limit = total_bytes * settings.warm_lru_pct / 100;
                /* Rescue ACTIVE items aggressively */
                if ((search->it_flags & ITEM_ACTIVE) != 0) {
                    search->it_flags &= ~ITEM_ACTIVE;
                    removed++;
                    if (cur_lru == WARM_LRU) {
                        itemstats[id].moves_within_lru++;
                        do_item_unlink_q(search, hv);
                        do_item_link_q(search, hv);
                        do_item_remove(search);
                        item_trylock_unlock(hold_lock);
                    } else {
                        /* Active HOT_LRU items flow to WARM */
                        itemstats[id].moves_to_warm++;
                        move_to_lru = WARM_LRU;
                        do_item_unlink_q(search, hv);
                        it = search;
                    }
                } else if (sizes_bytes[id] > limit ||
                           current_time - search->time > max_age) {
                    itemstats[id].moves_to_cold++;
                    move_to_lru = COLD_LRU;
                    do_item_unlink_q(search, hv);
                    it = search;
                    it_hv = hv;
                    removed++;
                    break;
                } else {
                    /* Don't want to move to COLD, not active, bail out */
                    it = search;
                    it_hv = hv;
                }
                break;
            case COLD_LRU:
            
                it = search; /* No matter what, we're stopping */
                #ifdef WITH_HILL
                    assert(it || !(flags & LRU_PULL_EVICT));
                #endif
                it_hv = hv;
                assert(it_hv != 0);
                if (flags & LRU_PULL_EVICT) {
                    if (settings.evict_to_free == 0) {
                        /* Don't think we need a counter for this. It'll OOM.  */
                        break;
                    }
                    itemstats[id].evicted++;
                    itemstats[id].evicted_time = current_time - search->time;
                    if (search->exptime != 0)
                        itemstats[id].evicted_nonzero++;
                    if ((search->it_flags & ITEM_FETCHED) == 0) {
                        itemstats[id].evicted_unfetched++;
                    }
                    if ((search->it_flags & ITEM_ACTIVE)) {
                        itemstats[id].evicted_active++;
                    }
                    LOGGER_LOG(NULL, LOG_EVICTIONS, LOGGER_EVICTION, search);
                    STORAGE_delete(ext_storage, search);
                    do_item_unlink_nolock(search, hv);
                    #ifdef WITH_HILL
                        assert(it || !(flags & LRU_PULL_EVICT));
                    #endif
                    removed++;
                    if (settings.slab_automove == 2) {
                        slabs_reassign(-1, orig_id);
                    }
                } else if (flags & LRU_PULL_RETURN_ITEM) {
                    /* Keep a reference to this item and return it. */
                    ret_it->it = it;
                    ret_it->hv = hv;
                } else if ((search->it_flags & ITEM_ACTIVE) != 0
                        && settings.lru_segmented) {
                    itemstats[id].moves_to_warm++;
                    search->it_flags &= ~ITEM_ACTIVE;
                    move_to_lru = WARM_LRU;
                    do_item_unlink_q(search, hv);
                    removed++;
                }
                break;
            case TEMP_LRU:
                it = search; /* Kill the loop. Parent only interested in reclaims */
                it_hv = hv;
                break;
        }
        if (it != NULL)
            break;
    }
    // assert(tries == 5);
#ifdef WITH_HILL
    // assert(it || !(flags & LRU_PULL_EVICT));
#endif
    pthread_mutex_unlock(&lru_locks[id]);

    if (it != NULL) {
        if (move_to_lru) {
            it->slabs_clsid = ITEM_clsid(it);
            it->slabs_clsid |= move_to_lru;
            item_link_q(it, it_hv);
        }
        if ((flags & LRU_PULL_RETURN_ITEM) == 0) {
            do_item_remove(it);
            item_trylock_unlock(hold_lock);
        }
    }

    return removed;
}

/* TODO: Third place this code needs to be deduped */
static void lru_bump_buf_link_q(lru_bump_buf *b) {
    pthread_mutex_lock(&bump_buf_lock);
    assert(b != bump_buf_head);

    b->prev = 0;
    b->next = bump_buf_head;
    if (b->next) b->next->prev = b;
    bump_buf_head = b;
    if (bump_buf_tail == 0) bump_buf_tail = b;
    pthread_mutex_unlock(&bump_buf_lock);
    return;
}

void *item_lru_bump_buf_create(void) {
    lru_bump_buf *b = calloc(1, sizeof(lru_bump_buf));
    if (b == NULL) {
        return NULL;
    }

    b->buf = bipbuf_new(sizeof(lru_bump_entry) * LRU_BUMP_BUF_SIZE);
    if (b->buf == NULL) {
        free(b);
        return NULL;
    }

    pthread_mutex_init(&b->mutex, NULL);

    lru_bump_buf_link_q(b);
    return b;
}

static bool lru_bump_async(lru_bump_buf *b, item *it, uint32_t hv) {
    bool ret = true;
    refcount_incr(it);
    pthread_mutex_lock(&b->mutex);
    lru_bump_entry *be = (lru_bump_entry *) bipbuf_request(b->buf, sizeof(lru_bump_entry));
    if (be != NULL) {
        be->it = it;
        be->hv = hv;
        if (bipbuf_push(b->buf, sizeof(lru_bump_entry)) == 0) {
            ret = false;
            b->dropped++;
        }
    } else {
        ret = false;
        b->dropped++;
    }
    if (!ret) {
        refcount_decr(it);
    }
    pthread_mutex_unlock(&b->mutex);
    return ret;
}

/* TODO: Might be worth a micro-optimization of having bump buffers link
 * themselves back into the central queue when queue goes from zero to
 * non-zero, then remove from list if zero more than N times.
 * If very few hits on cold this would avoid extra memory barriers from LRU
 * maintainer thread. If many hits, they'll just stay in the list.
 */
static bool lru_maintainer_bumps(void) {
    lru_bump_buf *b;
    lru_bump_entry *be;
    unsigned int size;
    unsigned int todo;
    bool bumped = false;
    pthread_mutex_lock(&bump_buf_lock);
    for (b = bump_buf_head; b != NULL; b=b->next) {
        pthread_mutex_lock(&b->mutex);
        be = (lru_bump_entry *) bipbuf_peek_all(b->buf, &size);
        pthread_mutex_unlock(&b->mutex);

        if (be == NULL) {
            continue;
        }
        todo = size;
        bumped = true;

        while (todo) {
            item_lock(be->hv);
            do_item_update(be->it);
            do_item_remove(be->it);
            item_unlock(be->hv);
            be++;
            todo -= sizeof(lru_bump_entry);
        }

        pthread_mutex_lock(&b->mutex);
        be = (lru_bump_entry *) bipbuf_poll(b->buf, size);
        pthread_mutex_unlock(&b->mutex);
    }
    pthread_mutex_unlock(&bump_buf_lock);
    return bumped;
}

static uint64_t lru_total_bumps_dropped(void) {
    uint64_t total = 0;
    lru_bump_buf *b;
    pthread_mutex_lock(&bump_buf_lock);
    for (b = bump_buf_head; b != NULL; b=b->next) {
        pthread_mutex_lock(&b->mutex);
        total += b->dropped;
        pthread_mutex_unlock(&b->mutex);
    }
    pthread_mutex_unlock(&bump_buf_lock);
    return total;
}

/* Loop up to N times:
 * If too many items are in HOT_LRU, push to COLD_LRU
 * If too many items are in WARM_LRU, push to COLD_LRU
 * If too many items are in COLD_LRU, poke COLD_LRU tail
 * 1000 loops with 1ms min sleep gives us under 1m items shifted/sec. The
 * locks can't handle much more than that. Leaving a TODO for how to
 * autoadjust in the future.
 */
static int lru_maintainer_juggle(const int slabs_clsid) {
    int i;
    int did_moves = 0;
    uint64_t total_bytes = 0;
    unsigned int chunks_perslab = 0;
    //unsigned int chunks_free = 0;
    /* TODO: if free_chunks below high watermark, increase aggressiveness */
    slabs_available_chunks(slabs_clsid, NULL,
            &chunks_perslab);
    if (settings.temp_lru) {
        /* Only looking for reclaims. Run before we size the LRU. */
        for (i = 0; i < 500; i++) {
            if (lru_pull_tail(slabs_clsid, TEMP_LRU, 0, 0, 0, NULL) <= 0) {
                break;
            } else {
                did_moves++;
            }
        }
    }

    rel_time_t cold_age = 0;
    rel_time_t hot_age = 0;
    rel_time_t warm_age = 0;
    /* If LRU is in flat mode, force items to drain into COLD via max age of 0 */
    if (settings.lru_segmented) {
        pthread_mutex_lock(&lru_locks[slabs_clsid|COLD_LRU]);
        if (tails[slabs_clsid|COLD_LRU]) {
            cold_age = current_time - tails[slabs_clsid|COLD_LRU]->time;
        }
        // Also build up total_bytes for the classes.
        total_bytes += sizes_bytes[slabs_clsid|COLD_LRU];
        pthread_mutex_unlock(&lru_locks[slabs_clsid|COLD_LRU]);

        hot_age = cold_age * settings.hot_max_factor;
        warm_age = cold_age * settings.warm_max_factor;

        // total_bytes doesn't have to be exact. cache it for the juggles.
        pthread_mutex_lock(&lru_locks[slabs_clsid|HOT_LRU]);
        total_bytes += sizes_bytes[slabs_clsid|HOT_LRU];
        pthread_mutex_unlock(&lru_locks[slabs_clsid|HOT_LRU]);

        pthread_mutex_lock(&lru_locks[slabs_clsid|WARM_LRU]);
        total_bytes += sizes_bytes[slabs_clsid|WARM_LRU];
        pthread_mutex_unlock(&lru_locks[slabs_clsid|WARM_LRU]);
    }

    /* Juggle HOT/WARM up to N times */
    for (i = 0; i < 500; i++) {
        int do_more = 0;
        if (lru_pull_tail(slabs_clsid, HOT_LRU, total_bytes, LRU_PULL_CRAWL_BLOCKS, hot_age, NULL) ||
            lru_pull_tail(slabs_clsid, WARM_LRU, total_bytes, LRU_PULL_CRAWL_BLOCKS, warm_age, NULL)) {
            do_more++;
        }
        if (settings.lru_segmented) {
            do_more += lru_pull_tail(slabs_clsid, COLD_LRU, total_bytes, LRU_PULL_CRAWL_BLOCKS, 0, NULL);
        }
        if (do_more == 0)
            break;
        did_moves++;
    }
    return did_moves;
}

/* Will crawl all slab classes a minimum of once per hour */
#define MAX_MAINTCRAWL_WAIT 60 * 60

/* Hoping user input will improve this function. This is all a wild guess.
 * Operation: Kicks crawler for each slab id. Crawlers take some statistics as
 * to items with nonzero expirations. It then buckets how many items will
 * expire per minute for the next hour.
 * This function checks the results of a run, and if it things more than 1% of
 * expirable objects are ready to go, kick the crawler again to reap.
 * It will also kick the crawler once per minute regardless, waiting a minute
 * longer for each time it has no work to do, up to an hour wait time.
 * The latter is to avoid newly started daemons from waiting too long before
 * retrying a crawl.
 */
static void lru_maintainer_crawler_check(struct crawler_expired_data *cdata, logger *l) {
    int i;
    static rel_time_t next_crawls[POWER_LARGEST];
    static rel_time_t next_crawl_wait[POWER_LARGEST];
    uint8_t todo[POWER_LARGEST];
    memset(todo, 0, sizeof(uint8_t) * POWER_LARGEST);
    bool do_run = false;
    unsigned int tocrawl_limit = 0;

    // TODO: If not segmented LRU, skip non-cold
    for (i = POWER_SMALLEST; i < POWER_LARGEST; i++) {
        crawlerstats_t *s = &cdata->crawlerstats[i];
        /* We've not successfully kicked off a crawl yet. */
        if (s->run_complete) {
            char *lru_name = "na";
            pthread_mutex_lock(&cdata->lock);
            int x;
            /* Should we crawl again? */
            uint64_t possible_reclaims = s->seen - s->noexp;
            uint64_t available_reclaims = 0;
            /* Need to think we can free at least 1% of the items before
             * crawling. */
            /* FIXME: Configurable? */
            uint64_t low_watermark = (possible_reclaims / 100) + 1;
            rel_time_t since_run = current_time - s->end_time;
            /* Don't bother if the payoff is too low. */
            for (x = 0; x < 60; x++) {
                available_reclaims += s->histo[x];
                if (available_reclaims > low_watermark) {
                    if (next_crawl_wait[i] < (x * 60)) {
                        next_crawl_wait[i] += 60;
                    } else if (next_crawl_wait[i] >= 60) {
                        next_crawl_wait[i] -= 60;
                    }
                    break;
                }
            }

            if (available_reclaims == 0) {
                next_crawl_wait[i] += 60;
            }

            if (next_crawl_wait[i] > MAX_MAINTCRAWL_WAIT) {
                next_crawl_wait[i] = MAX_MAINTCRAWL_WAIT;
            }

            next_crawls[i] = current_time + next_crawl_wait[i] + 5;
            switch (GET_LRU(i)) {
                case HOT_LRU:
                    lru_name = "hot";
                    break;
                case WARM_LRU:
                    lru_name = "warm";
                    break;
                case COLD_LRU:
                    lru_name = "cold";
                    break;
                case TEMP_LRU:
                    lru_name = "temp";
                    break;
            }
            LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_CRAWLER_STATUS, NULL,
                    CLEAR_LRU(i),
                    lru_name,
                    (unsigned long long)low_watermark,
                    (unsigned long long)available_reclaims,
                    (unsigned int)since_run,
                    next_crawls[i] - current_time,
                    s->end_time - s->start_time,
                    s->seen,
                    s->reclaimed);
            // Got our calculation, avoid running until next actual run.
            s->run_complete = false;
            pthread_mutex_unlock(&cdata->lock);
        }
        if (current_time > next_crawls[i]) {
            pthread_mutex_lock(&lru_locks[i]);
            if (sizes[i] > tocrawl_limit) {
                tocrawl_limit = sizes[i];
            }
            pthread_mutex_unlock(&lru_locks[i]);
            todo[i] = 1;
            do_run = true;
            next_crawls[i] = current_time + 5; // minimum retry wait.
        }
    }
    if (do_run) {
        if (settings.lru_crawler_tocrawl && settings.lru_crawler_tocrawl < tocrawl_limit) {
            tocrawl_limit = settings.lru_crawler_tocrawl;
        }
        lru_crawler_start(todo, tocrawl_limit, CRAWLER_AUTOEXPIRE, cdata, NULL, 0);
    }
}

slab_automove_reg_t slab_automove_default = {
    .init = slab_automove_init,
    .free = slab_automove_free,
    .run = slab_automove_run
};
#ifdef EXTSTORE
slab_automove_reg_t slab_automove_extstore = {
    .init = slab_automove_extstore_init,
    .free = slab_automove_extstore_free,
    .run = slab_automove_extstore_run
};
#endif
static pthread_t lru_maintainer_tid;

#define MAX_LRU_MAINTAINER_SLEEP 1000000
#define MIN_LRU_MAINTAINER_SLEEP 1000

static void *lru_maintainer_thread(void *arg) {
    slab_automove_reg_t *sam = &slab_automove_default;
#ifdef EXTSTORE
    void *storage = arg;
    if (storage != NULL)
        sam = &slab_automove_extstore;
#endif
    int i;
    useconds_t to_sleep = MIN_LRU_MAINTAINER_SLEEP;
    useconds_t last_sleep = MIN_LRU_MAINTAINER_SLEEP;
    rel_time_t last_crawler_check = 0;
    rel_time_t last_automove_check = 0;
    useconds_t next_juggles[MAX_NUMBER_OF_SLAB_CLASSES] = {0};
    useconds_t backoff_juggles[MAX_NUMBER_OF_SLAB_CLASSES] = {0};
    struct crawler_expired_data *cdata =
        calloc(1, sizeof(struct crawler_expired_data));
    if (cdata == NULL) {
        fprintf(stderr, "Failed to allocate crawler data for LRU maintainer thread\n");
        abort();
    }
    pthread_mutex_init(&cdata->lock, NULL);
    cdata->crawl_complete = true; // kick off the crawler.
    logger *l = logger_create();
    if (l == NULL) {
        fprintf(stderr, "Failed to allocate logger for LRU maintainer thread\n");
        abort();
    }

    double last_ratio = settings.slab_automove_ratio;
    void *am = sam->init(&settings);

    pthread_mutex_lock(&lru_maintainer_lock);
    if (settings.verbose > 2)
        fprintf(stderr, "Starting LRU maintainer background thread\n");
    while (do_run_lru_maintainer_thread) {
        pthread_mutex_unlock(&lru_maintainer_lock);
        if (to_sleep)
            usleep(to_sleep);
        pthread_mutex_lock(&lru_maintainer_lock);
        /* A sleep of zero counts as a minimum of a 1ms wait */
        last_sleep = to_sleep > 1000 ? to_sleep : 1000;
        to_sleep = MAX_LRU_MAINTAINER_SLEEP;

        STATS_LOCK();
        stats.lru_maintainer_juggles++;
        STATS_UNLOCK();

        /* Each slab class gets its own sleep to avoid hammering locks */
        for (i = POWER_SMALLEST; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
            next_juggles[i] = next_juggles[i] > last_sleep ? next_juggles[i] - last_sleep : 0;

            if (next_juggles[i] > 0) {
                // Sleep the thread just for the minimum amount (or not at all)
                if (next_juggles[i] < to_sleep)
                    to_sleep = next_juggles[i];
                continue;
            }

            int did_moves = lru_maintainer_juggle(i);
            if (did_moves == 0) {
                if (backoff_juggles[i] != 0) {
                    backoff_juggles[i] += backoff_juggles[i] / 8;
                } else {
                    backoff_juggles[i] = MIN_LRU_MAINTAINER_SLEEP;
                }
                if (backoff_juggles[i] > MAX_LRU_MAINTAINER_SLEEP)
                    backoff_juggles[i] = MAX_LRU_MAINTAINER_SLEEP;
            } else if (backoff_juggles[i] > 0) {
                backoff_juggles[i] /= 2;
                if (backoff_juggles[i] < MIN_LRU_MAINTAINER_SLEEP) {
                    backoff_juggles[i] = 0;
                }
            }
            next_juggles[i] = backoff_juggles[i];
            if (next_juggles[i] < to_sleep)
                to_sleep = next_juggles[i];
        }

        /* Minimize the sleep if we had async LRU bumps to process */
        if (settings.lru_segmented && lru_maintainer_bumps() && to_sleep > 1000) {
            to_sleep = 1000;
        }

        /* Once per second at most */
        if (settings.lru_crawler && last_crawler_check != current_time) {
            lru_maintainer_crawler_check(cdata, l);
            last_crawler_check = current_time;
        }

        if (settings.slab_automove == 1 && last_automove_check != current_time) {
            if (last_ratio != settings.slab_automove_ratio) {
                sam->free(am);
                am = sam->init(&settings);
                last_ratio = settings.slab_automove_ratio;
            }
            int src, dst;
            sam->run(am, &src, &dst);
            if (src != -1 && dst != -1) {
                slabs_reassign(src, dst);
                LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_SLAB_MOVE, NULL,
                        src, dst);
            }
            // dst == 0 means reclaim to global pool, be more aggressive
            if (dst != 0) {
                last_automove_check = current_time;
            } else if (dst == 0) {
                // also ensure we minimize the thread sleep
                to_sleep = 1000;
            }
        }
    }
    pthread_mutex_unlock(&lru_maintainer_lock);
    sam->free(am);
    // LRU crawler *must* be stopped.
    free(cdata);
    if (settings.verbose > 2)
        fprintf(stderr, "LRU maintainer thread stopping\n");

    return NULL;
}

int stop_lru_maintainer_thread(void) {
    int ret;
    pthread_mutex_lock(&lru_maintainer_lock);
    /* LRU thread is a sleep loop, will die on its own */
    do_run_lru_maintainer_thread = 0;
    pthread_mutex_unlock(&lru_maintainer_lock);
    if ((ret = pthread_join(lru_maintainer_tid, NULL)) != 0) {
        fprintf(stderr, "Failed to stop LRU maintainer thread: %s\n", strerror(ret));
        return -1;
    }
    settings.lru_maintainer_thread = false;
    return 0;
}

int start_lru_maintainer_thread(void *arg) {
    int ret;

    pthread_mutex_lock(&lru_maintainer_lock);
    do_run_lru_maintainer_thread = 1;
    settings.lru_maintainer_thread = true;
    if ((ret = pthread_create(&lru_maintainer_tid, NULL,
        lru_maintainer_thread, arg)) != 0) {
        fprintf(stderr, "Can't create LRU maintainer thread: %s\n",
            strerror(ret));
        pthread_mutex_unlock(&lru_maintainer_lock);
        return -1;
    }
    thread_setname(lru_maintainer_tid, "mc-lrumaint");
    pthread_mutex_unlock(&lru_maintainer_lock);

    return 0;
}

/* If we hold this lock, crawler can't wake up or move */
void lru_maintainer_pause(void) {
    pthread_mutex_lock(&lru_maintainer_lock);
}

void lru_maintainer_resume(void) {
    pthread_mutex_unlock(&lru_maintainer_lock);
}

/* Tail linkers and crawler for the LRU crawler. */
void do_item_linktail_q(item *it) { /* item is the new tail */
    item **head, **tail;
    assert(it->it_flags == 1);
    assert(it->nbytes == 0);

    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];
    //assert(*tail != 0);
    assert(it != *tail);
    assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = *tail;
    it->next = 0;
    if (it->prev) {
        assert(it->prev->next == 0);
        it->prev->next = it;
    }
    *tail = it;
    if (*head == 0) *head = it;
    return;
}

void do_item_unlinktail_q(item *it) {
    item **head, **tail;
    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];

    if (*head == it) {
        assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        assert(it->next == 0);
        *tail = it->prev;
    }
    assert(it->next != it);
    assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    return;
}

/* This is too convoluted, but it's a difficult shuffle. Try to rewrite it
 * more clearly. */
item *do_item_crawl_q(item *it) {
    item **head, **tail;
    assert(it->it_flags == 1);
    assert(it->nbytes == 0);
    head = &heads[it->slabs_clsid];
    tail = &tails[it->slabs_clsid];

    /* We've hit the head, pop off */
    if (it->prev == 0) {
        assert(*head == it);
        if (it->next) {
            *head = it->next;
            assert(it->next->prev == it);
            it->next->prev = 0;
        }
        return NULL; /* Done */
    }

    /* Swing ourselves in front of the next item */
    /* NB: If there is a prev, we can't be the head */
    assert(it->prev != it);
    if (it->prev) {
        if (*head == it->prev) {
            /* Prev was the head, now we're the head */
            *head = it;
        }
        if (*tail == it) {
            /* We are the tail, now they are the tail */
            *tail = it->prev;
        }
        assert(it->next != it);
        if (it->next) {
            assert(it->prev->next == it);
            it->prev->next = it->next;
            it->next->prev = it->prev;
        } else {
            /* Tail. Move this above? */
            it->prev->next = 0;
        }
        /* prev->prev's next is it->prev */
        it->next = it->prev;
        it->prev = it->next->prev;
        it->next->prev = it;
        /* New it->prev now, if we're not at the head. */
        if (it->prev) {
            it->prev->next = it;
        }
    }
    assert(it->next != it);
    assert(it->prev != it);

    return it->next; /* success */
}

#ifdef WITH_HILL
ghost_item* find_ghost_item(uint32_t hv, uint32_t hv2, bool sim /*false*/)
{
    // mutex or not?
    ghost_item *gitem;
    if (sim) {
        pthread_mutex_lock(&sim_ghost_hashtable_mutex[hv & ghost_hash_mask]);
        gitem = sim_ghost_hashtable[hv & ghost_hash_mask];
        while(gitem && gitem->hv2 != hv2) {
            gitem = gitem->hnext;
        }
        pthread_mutex_unlock(&sim_ghost_hashtable_mutex[hv & ghost_hash_mask]);
    } else {
        pthread_mutex_lock(&ghost_hashtable_mutex[hv & ghost_hash_mask]);
        gitem = ghost_hashtable[hv & ghost_hash_mask];
        while(gitem && gitem->hv2 != hv2) {
            gitem = gitem->hnext;
        }
        pthread_mutex_unlock(&ghost_hashtable_mutex[hv & ghost_hash_mask]);
    }
    // assert(gitem && gitem->hv2);
    return gitem;
}

ghost_item* ghost_item_alloc(void)
{
    // ghost_item* ret = calloc(1, sizeof(ghost_item));
    // memset(ret, 0, sizeof(ghost_item));
    // return ret;
    pthread_mutex_lock(&ghost_item_buffer_lock);
    ghost_item* ret;
    if (ghost_item_buffer_head == NULL) {
        ghost_item_buffer_head = calloc(GITEM_PER_ALLOC, sizeof(ghost_item));
        ghost_item* cur = ghost_item_buffer_head;
        for (int i = 0; i < GITEM_PER_ALLOC; i++) {
            ghost_item* nxt = cur + 1;
            if (i != GITEM_PER_ALLOC - 1) {
                cur->gnext = nxt;
                nxt->gprev = cur;
            } else {
                cur->gnext = NULL;
            }
            cur = nxt;
        }
    }
    ret = ghost_item_buffer_head;
    ghost_item_buffer_head = ghost_item_buffer_head->gnext;
    if (ghost_item_buffer_head) {
        ghost_item_buffer_head->gprev = NULL;
    }
    pthread_mutex_unlock(&ghost_item_buffer_lock);
    return ret;
}

void ghost_item_free(ghost_item* git)
{
    pthread_mutex_lock(&ghost_item_buffer_lock);
    assert(git->gprev == NULL || git->gprev->gnext != git);
    assert(git->gnext == NULL || git->gnext->gprev != git);
    git->gprev = git->hnext = NULL;
    git->inserted_lv = git->inserted_ts = 0;
    git->gnext = ghost_item_buffer_head;
    git->hv = git->hv2 = 0;
    git->slabs_clsid = 0;
    ghost_item_buffer_head = git;
    pthread_mutex_unlock(&ghost_item_buffer_lock);
}

void ghost_item_insert(ghost_item* git, uint32_t hv, uint32_t hv2, bool sim/* false */)
{
    if (sim) {
        // need this mutex?
        pthread_mutex_lock(&sim_ghost_hashtable_mutex[hv & ghost_hash_mask]);
        git->hnext = sim_ghost_hashtable[hv & ghost_hash_mask];
        sim_ghost_hashtable[hv & ghost_hash_mask] = git;
        pthread_mutex_unlock(&sim_ghost_hashtable_mutex[hv & ghost_hash_mask]);
    } else {
        pthread_mutex_lock(&ghost_hashtable_mutex[hv & ghost_hash_mask]);
        git->hnext = ghost_hashtable[hv & ghost_hash_mask];
        ghost_hashtable[hv & ghost_hash_mask] = git;
        pthread_mutex_unlock(&ghost_hashtable_mutex[hv & ghost_hash_mask]);
    }
}

void ghost_item_insert_maintain(void* hill, ghost_item* git, uint8_t id, bool sim/* false */)
{
    if (sim) {
        hill_sim_t* hill_sim = (hill_sim_t*)hill;
        while (hill_sim->gsize > 0 && hill_sim->gsize + 1 > GHOST_CACHE_RATIO * hill_sim->total_size + GHOST_CACHE_RATIO)
        {
            ghost_item_lru_pop((void *)hill_sim, id, sim);
        }
        assert(GHOST_CACHE_RATIO * hill_sim->total_size + GHOST_CACHE_RATIO >= hill_sim->gsize + 1);
        ghost_item_lru_push((void *)hill_sim, git, id, sim);
    } else {
        hill_t* ori_hill = (hill_t*)hill;
        while (ori_hill->gsize > 0 && ori_hill->gsize + 1 > GHOST_CACHE_RATIO * ori_hill->total_size + GHOST_CACHE_RATIO)
        {
            ghost_item_lru_pop(ori_hill, id, sim);
        }
        assert(GHOST_CACHE_RATIO * ori_hill->total_size + GHOST_CACHE_RATIO >= ori_hill->gsize + 1);
        ghost_item_lru_push(ori_hill, git, id, sim);
    }
}

void ghost_item_remove(ghost_item* git, uint32_t hv, uint32_t hv2, bool sim/* false */)
{
    if (sim) {
        pthread_mutex_lock(&sim_ghost_hashtable_mutex[hv & ghost_hash_mask]);
        ghost_item *gitem = sim_ghost_hashtable[hv & ghost_hash_mask];
        if (gitem && gitem->hv2 == hv2) {
            sim_ghost_hashtable[hv & ghost_hash_mask] = gitem->hnext;
            pthread_mutex_unlock(&sim_ghost_hashtable_mutex[hv & ghost_hash_mask]);
            return;
        }
        while (gitem->hnext && gitem->hnext->hv2 != hv2) {
            gitem = gitem->hnext;
        }
        assert(gitem->hnext && gitem->hnext->hv2 == hv2);
        gitem->hnext = gitem->hnext->hnext;
        pthread_mutex_unlock(&sim_ghost_hashtable_mutex[hv & ghost_hash_mask]);
    } else {
        pthread_mutex_lock(&ghost_hashtable_mutex[hv & ghost_hash_mask]);
        ghost_item *gitem = ghost_hashtable[hv & ghost_hash_mask];
        if (gitem && gitem->hv2 == hv2) {
            ghost_hashtable[hv & ghost_hash_mask] = gitem->hnext;
            pthread_mutex_unlock(&ghost_hashtable_mutex[hv & ghost_hash_mask]);
            return;
        }
        while (gitem->hnext && gitem->hnext->hv2 != hv2) {
            gitem = gitem->hnext;
        }
        // assert(1 == 2);
        assert(gitem->hnext && gitem->hnext->hv2 == hv2);
        gitem->hnext = gitem->hnext->hnext;
        pthread_mutex_unlock(&ghost_hashtable_mutex[hv & ghost_hash_mask]);
    }
}

void ghost_item_remove_maintain(void* hill, ghost_item* git, uint8_t id, bool sim/* false */)
{
    if (sim) {
        hill_sim_t* hill_sim = (hill_sim_t*)hill;
        itemstats[id].sim_ghost_size--;
        hill_sim->gsize--;
        if (git == hill_sim->ghead) {
            hill_sim->ghead = git->gnext;
        }
        if (git == hill_sim->gtail) {
            hill_sim->gtail = git->gprev;
        }
        if (git->gprev) {
            git->gprev->gnext = git->gnext;
        }
        if (git->gnext) {
            git->gnext->gprev = git->gprev;
        }
        assert(git->gprev == NULL || git->gprev->gnext != git);
        assert(git->gnext == NULL || git->gnext->gprev != git);
    } else {
        hill_t* ori_hill = (hill_t*)hill;
        itemstats[id].ghost_size--;
        assert(ori_hill->gsize > 0);
        ori_hill->gsize--;
        if (git == ori_hill->ghead) {
            ori_hill->ghead = git->gnext;
        }
        if (git == ori_hill->gtail) {
            ori_hill->gtail = git->gprev;
        }
        if (git->gprev) {
            git->gprev->gnext = git->gnext;
        }
        if (git->gnext) {
            git->gnext->gprev = git->gprev;
        }
    }
}

void ghost_item_lru_pop(void* hill, uint8_t id, bool sim)
{
    if (sim) {
        hill_sim_t* hill_sim = (hill_sim_t*)hill;
        itemstats[id].sim_ghost_size--;
        assert(hill_sim->gsize > 0);
        hill_sim->gsize--;
        ghost_item* git = hill_sim->gtail;
        if (git == hill_sim->ghead) {
            hill_sim->ghead = git->gnext;
        }
        if (git == hill_sim->gtail) {
            hill_sim->gtail = git->gprev;
        }
        if (git->gprev) {
            git->gprev->gnext = git->gnext;
        }
        if (git->gnext) {
            git->gnext->gprev = git->gprev;
        }
        ghost_item_remove(git, git->hv, git->hv2, sim);
        ghost_item_free(git);
    } else {
        hill_t* ori_hill = (hill_t*)hill;
        itemstats[id].ghost_size--;
        assert(ori_hill->gsize > 0);
        ori_hill->gsize--;
        ghost_item* git = ori_hill->gtail;
        if (git == ori_hill->ghead) {
            ori_hill->ghead = git->gnext;
        }
        if (git == ori_hill->gtail) {
            ori_hill->gtail = git->gprev;
        }
        if (git->gprev) {
            git->gprev->gnext = git->gnext;
        }
        if (git->gnext) {
            git->gnext->gprev = git->gprev;
        }
        ghost_item_remove(git, git->hv, git->hv2, sim);
        ghost_item_free(git);
    }
}

void ghost_item_lru_push(void* hill, ghost_item* git, uint8_t id, bool sim)
{
    if (sim) {
        hill_sim_t* hill_sim = (hill_sim_t*)hill;
        itemstats[id].sim_ghost_size++;
        hill_sim->gsize++;
        git->gnext = hill_sim->ghead;
        hill_sim->ghead = git;
        if (git->gnext) {
            git->gnext->gprev = git;
        }
        if (hill_sim->gtail == NULL) hill_sim->gtail = git;
        git->gprev = NULL;
    } else {
        hill_t* ori_hill = (hill_t*)hill;
        itemstats[id].ghost_size++;
        ori_hill->gsize++;
        git->gnext = ori_hill->ghead;
        ori_hill->ghead = git;
        if (git->gnext) {
            git->gnext->gprev = git;
        }
        if (ori_hill->gtail == NULL) ori_hill->gtail = git;
        git->gprev = NULL;
    }
}

bool simulator_access(const char *key, const size_t nkey, const uint32_t hv, uint8_t slabclass_id)
{
    ghost_item* git = sim_assoc_find(key, nkey, hv);
    bool in_cache = 0;
    if (git) {
        in_cache = 1;
        // bump git
        // hills_sim
    } else {

    }
    return in_cache;
}

ghost_item* sim_assoc_find(const char *key, const size_t nkey, const uint32_t hv)
{
    pthread_mutex_lock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
    ghost_item* git = sim_hashtable[hv & ghost_hash_mask];
    uint32_t hv2 = hash2(key, nkey);
    while (git != NULL && git->hv2 != hv2) {
        git = git->hnext;
    }
    pthread_mutex_unlock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
    return git;
}

ghost_item* sim_assoc_find_byhv(const uint32_t hv, const uint32_t hv2)
{
    pthread_mutex_lock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
    ghost_item* git = sim_hashtable[hv & ghost_hash_mask];
    while (git != NULL && git->hv2 != hv2) {
        git = git->hnext;
    }
    pthread_mutex_unlock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
    return git;
}

void sim_assoc_remove(ghost_item* git, uint32_t hv, uint32_t hv2)
{
    pthread_mutex_lock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
    ghost_item *gitem = sim_hashtable[hv & ghost_hash_mask];
    if (gitem && gitem->hv2 == hv2) {
        sim_hashtable[hv & ghost_hash_mask] = gitem->hnext;
        pthread_mutex_unlock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
        return;
    }
    while (gitem->hnext && gitem->hnext->hv2 != hv2) {
        gitem = gitem->hnext;
    }
    assert(gitem->hnext && gitem->hnext->hv2 == hv2);
    gitem->hnext = gitem->hnext->hnext;
    pthread_mutex_unlock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
}

ghost_item* assoc_insert_sim(ghost_item* git)
{
    uint32_t hv = git->hv;
    pthread_mutex_lock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
    git->hnext = sim_hashtable[git->hv & ghost_hash_mask];
    sim_hashtable[git->hv & ghost_hash_mask] = git;
    pthread_mutex_unlock(&sim_hashtable_mutex[hv & ghost_hash_mask]);
    return git;
}

void pull_tail_sim(uint8_t id)
{
    hill_sim_t* hill = &hills_sim[id];
    uint32_t lowest = 0;
    for (; lowest < HILL_MAX_LEVEL; lowest++) {
        if (hill->heads[lowest] != NULL) {
            break;
        }
    }
    ghost_item* search;
    if (lowest < HILL_MAX_LEVEL)
        search = hill->heads[lowest];
    else {
        search = NULL;
    }
    // assert(search);
    if (search) {
        assert(sim_assoc_find_byhv(search->hv, search->hv2) != NULL);
        do_item_unlink_q_sim(search);
    }
}

#endif
