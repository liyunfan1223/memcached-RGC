#define HOT_LRU 0
#define WARM_LRU 64
#define COLD_LRU 128
#define TEMP_LRU 192

#define CLEAR_LRU(id) (id & ~(3<<6))
#define GET_LRU(id) (id & (3<<6))

/* See items.c */
uint64_t get_cas_id(void);
void set_cas_id(uint64_t new_cas);

/*@null@*/
item *do_item_alloc(const char *key, const size_t nkey, const unsigned int flags, const rel_time_t exptime, const int nbytes);
item_chunk *do_item_alloc_chunk(item_chunk *ch, const size_t bytes_remain);
item *do_item_alloc_pull(const size_t ntotal, const unsigned int id);
void item_free(item *it);
bool item_size_ok(const size_t nkey, const int flags, const int nbytes);

int  do_item_link(item *it, const uint32_t hv);     /** may fail if transgresses limits */
#ifdef WITH_HILL
int do_item_link_sim(ghost_item *git, const uint8_t slabs_clsid);
void do_item_link_q_sim(ghost_item* git, uint8_t slabs_clsid);
#endif
void do_item_unlink(item *it, const uint32_t hv);
void do_item_unlink_nolock(item *it, const uint32_t hv);
void do_item_remove(item *it);
void do_item_update(item *it);   /** update LRU time to current and reposition */
void do_item_update_nolock(item *it);
int  do_item_replace(item *it, item *new_it, const uint32_t hv);
void do_item_link_fixup(item *it);

int item_is_flushed(item *it);
unsigned int do_get_lru_size(uint32_t id);

void do_item_linktail_q(item *it);
void do_item_unlinktail_q(item *it);
item *do_item_crawl_q(item *it);

void *item_lru_bump_buf_create(void);

#define LRU_PULL_EVICT 1
#define LRU_PULL_CRAWL_BLOCKS 2
#define LRU_PULL_RETURN_ITEM 4 /* fill info struct if available */

#ifdef WITH_HILL
#define HILL_MAX_BITS 6
#define HILL_MAX_DECAY_TS HILL_MAX_BITS
#define HILL_MAX_LEVEL (1 << HILL_MAX_BITS)
#define DEFAULT_INSERTED_LEVEL (1 << 0)
#define EST_SLABS 4
// #define EST_ITEM_COUNTS (1 << 16)
// #define GHOST_HASHSIZE (EST_ITEM_COUNTS << 2)
// #define ghost_hash_mask (GHOST_HASHSIZE - 1)
#define TOP_LRU_TS (1 << 31)
#define GHOST_CACHE_RATIO 4
#define ORIGINAL_DECAY_INTERVAL (EST_ITEM_COUNTS << 4) // 默认cur_half=16
#define SIMULATOR_DECAY_RATIO 1.5f
// #define SIMULATOR_DECAY_RATIO 1.0f
#define GITEM_PER_ALLOC 1024
#define EPSILON (1e-8)
#define LAMBDA 1
#endif

struct lru_pull_tail_return {
    item *it;
    uint32_t hv;
};

#ifdef WITH_HILL
struct hill_pull_tail_return {
    item *it;
    uint32_t hv;
};

/* Yunfan */
typedef struct _hill_t {
    item* heads[HILL_MAX_LEVEL];
    item* tails[HILL_MAX_LEVEL];
    uint32_t lowest_level_non_empty;
    uint64_t access_ts;
    uint32_t decay_ts[HILL_MAX_DECAY_TS];
    uint32_t size[HILL_MAX_LEVEL];
    uint64_t decay_interval;
    uint32_t next_decay_ts;
    uint32_t prev_decay_ts;
    item* top_head;
    item* top_tail;
    uint32_t top_lru_size;
    uint32_t top_lru_max_size;
    uint32_t update_interval;
    uint32_t total_size;
    uint32_t gsize;
    uint32_t last_update_hits;
    uint32_t last_update_hits_sim;
    uint32_t last_update_misses;
    uint32_t last_update_misses_sim;
    uint32_t stable_count;
    ghost_item* ghead;
    ghost_item* gtail;
} hill_t;

typedef struct _hill_sim_t {
    ghost_item* heads[HILL_MAX_LEVEL];
    ghost_item* tails[HILL_MAX_LEVEL];
    uint32_t lowest_level_non_empty;
    uint32_t access_ts;
    uint32_t decay_ts[HILL_MAX_DECAY_TS];
    uint32_t size[HILL_MAX_LEVEL];
    uint64_t decay_interval;
    uint32_t next_decay_ts;
    uint32_t prev_decay_ts;
    ghost_item* top_head;
    ghost_item* top_tail;
    uint32_t top_lru_size;
    uint32_t top_lru_max_size;
    uint32_t update_interval;
    uint32_t total_size;
    uint32_t gsize;
    uint32_t interval_hit;
    ghost_item* ghead;
    ghost_item* gtail;
} hill_sim_t;

#endif

int lru_pull_tail(const int orig_id, const int cur_lru,
        const uint64_t total_bytes, const uint8_t flags, const rel_time_t max_age,
        struct lru_pull_tail_return *ret_it);

// /* Yunfan */
// int hill_pull_tail(const int orig_id, const uint64_t total_bytes, const uint8_t flags, const rel_time_t max_age,
//         struct hill_pull_tail_return *ret_it);

/*@null@*/
char *item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
void item_stats(ADD_STAT add_stats, void *c);
void do_item_stats_add_crawl(const int i, const uint64_t reclaimed,
        const uint64_t unfetched, const uint64_t checked);
void item_stats_totals(ADD_STAT add_stats, void *c);
/*@null@*/
void item_stats_sizes(ADD_STAT add_stats, void *c);
void item_stats_sizes_init(void);
void item_stats_sizes_enable(ADD_STAT add_stats, void *c);
void item_stats_sizes_disable(ADD_STAT add_stats, void *c);
void item_stats_sizes_add(item *it);
void item_stats_sizes_remove(item *it);
bool item_stats_sizes_status(void);

/* stats getter for slab automover */
typedef struct {
    int64_t evicted;
    int64_t outofmemory;
    uint32_t age;
} item_stats_automove;
void fill_item_stats_automove(item_stats_automove *am);

item *do_item_get(const char *key, const size_t nkey, const uint32_t hv, LIBEVENT_THREAD *t, const bool do_update);
item *do_item_touch(const char *key, const size_t nkey, uint32_t exptime, const uint32_t hv, LIBEVENT_THREAD *t);
void do_item_bump(LIBEVENT_THREAD *t, item *it, const uint32_t hv);
void item_stats_reset(void);
void hill_init(void);
extern pthread_mutex_t lru_locks[POWER_LARGEST];
extern pthread_mutex_t sim_locks[POWER_LARGEST];

int start_lru_maintainer_thread(void *arg);
int stop_lru_maintainer_thread(void);
void lru_maintainer_pause(void);
void lru_maintainer_resume(void);

void *lru_bump_buf_create(void);

#ifdef WITH_HILL
void do_item_unlink_q_sim(ghost_item *it);
ghost_item* ghost_item_alloc(void);
ghost_item* find_ghost_item(uint32_t hv, uint32_t hv2, bool sim);
// ghost_item* delete_ghost_item(uint32_t hv, uint32_t hv2);
void ghost_item_insert(ghost_item* git, uint32_t hv, uint32_t hv2, bool sim);
void ghost_item_insert_maintain(void* hill, ghost_item* git, uint8_t id, bool sim);
void ghost_item_remove(ghost_item* git, uint32_t hv, uint32_t hv2, bool sim);
void ghost_item_remove_maintain(void* hill, ghost_item* git, uint8_t id, bool sim);
void ghost_item_free(ghost_item* git);
void ghost_item_lru_pop(void* hill, uint8_t id, bool sim);
void ghost_item_lru_push(void* hill, ghost_item* git, uint8_t id, bool sim);
bool simulator_access(const char *key, const size_t nkey, const uint32_t hv, uint8_t slabclass_id);
ghost_item* sim_assoc_find(const char *key, const size_t nkey, const uint32_t hv);
ghost_item* sim_assoc_find_byhv(const uint32_t hv, const uint32_t hv2);
void sim_assoc_remove(ghost_item* git, uint32_t hv, uint32_t hv2);
ghost_item* assoc_insert_sim(ghost_item* git);
void pull_tail_sim(uint8_t id);
#endif