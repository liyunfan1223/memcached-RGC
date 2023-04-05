import memcache
import random
import math
import time
# 127.0.0.1 代表服务器IP地址，因为我们得memcached安装在本地，所以其IP地址为127.0.0.1
# 11211 代表端口号，memcached默认端口号为 11211
# debug=True 这个表示开启调试模式
mc = memcache.Client(["127.0.0.1:11211"],debug=True)
# set() 第1个值为key[必须];第2个值为value[必须];第三个值为存活时间[可选]，默认为3小时;关于set更多参数可点击set进去查看

miss = 0
hit = 0
kBigPrime = 0x5bd1e995
kBigInt = 1 << 62
mp = {}
# print(kBigPrime, kBigInt)

def access_set(key):
    mc.set(key, "value")

def access_get(key):
    value = mc.get(key)
    # print(value)
    if value is None:
        global miss
        miss += 1
    else:
        global hit
        hit += 1

def gen_rand_skewed(skewed, size):
    order = -random.uniform(0, 1) * skewed
    exp_ran = math.exp(order)
    rand_num = int(exp_ran * size)
    return int(rand_num * kBigPrime % size)

if __name__ == "__main__":
    mc.flush_all()
    cur_time = time.time()
    high_freq = 50
    low_freq = 1024 * 3000
    test_times = 1024 * 300

    # for i in range(20):
    #     for j in range(high_freq):
    #         access_set(str(j))
    
    # for j in range(test_times):
    #     access_set(str(j))
    
    # for j in range(high_freq):
    #     access_get(str(j))
    for i in range(test_times):
        # print(random.randint(0, 2))
        # if (random.randint(0, 3) == 0):
        #     key = random.randint(0, high_freq)
        # else:
        #     key = random.randint(0, low_freq)
        key = gen_rand_skewed(12, low_freq)
        if key in mp:
            access_get(str(key))
        else:
            mp[key] = 1
            access_set(str(key))

    print(hit, miss)
    print(f"hit ratio: {0 if (miss + hit) == 0 else hit / (miss + hit) * 100}%")
    end_time = time.time()
    print(f"spend time: {end_time - cur_time}s")
