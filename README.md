# MemHC

MemHC is a high performance multithreaded event-based key/value cache
store, modified based on Memcached. MemHC use Hill-Cache cache replacement policy to help the system achieve better hit rate results.

## Dependencies

* libevent - https://www.monkey.org/~provos/libevent/ (libevent-dev)
* libseccomp (optional, experimental, linux) - enables process restrictions for
  better security. Tested only on x86-64 architectures.
* openssl (optional) - enables TLS support. need relatively up to date
  version. pkg-config is needed to find openssl dependencies (such as -lz).


## Build
```
./configure --prefix=/path/to/memhc
make -j$(nproc) && make install
```

## Environment

Be warned that the -k (mlockall) option to memcached might be
dangerous when using a large cache. Just make sure the memcached machines
don't swap.  memcached does non-blocking network I/O, but not disk.  (it
should never go to disk, or you've lost the whole point of it)

## Build status

See https://build.memcached.org/ for multi-platform regression testing status.

## Bug reports

Feel free to use the issue tracker on github.

**If you are reporting a security bug** please contact a maintainer privately.
We follow responsible disclosure: we handle reports privately, prepare a
patch, allow notifications to vendor lists. Then we push a fix release and your
bug can be posted publicly with credit in our release notes and commit
history.

## Website

* https://www.memcached.org

## Contributing

See https://github.com/memcached/memcached/wiki/DevelopmentRepos
