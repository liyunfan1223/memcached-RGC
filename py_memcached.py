import memcache
import random
import math
import time
# 127.0.0.1 代表服务器IP地址，因为我们得memcached安装在本地，所以其IP地址为127.0.0.1
# 11211 代表端口号，memcached默认端口号为 11211
# debug=True 这个表示开启调试模式
mc = memcache.Client(["127.0.0.1:11211"],debug=True,socket_timeout=600)
# set() 第1个值为key[必须];第2个值为value[必须];第三个值为存活时间[可选]，默认为3小时;关于set更多参数可点击set进去查看

random.seed(42)
miss = 0
hit = 0
kBigPrime = 0x5bd1e995
kBigInt = 1 << 62
mp = {}
# print(kBigPrime, kBigInt)
value_len = 64 * 1024
value_to_set = ''
# for i in range(value_len):
value_to_set = 'v' * value_len
# print(value_to_set)

def report():
    global hit, miss
    print(f"hit: {hit}, miss: {miss}, hit ratio: {0 if (miss + hit) == 0 else hit / (miss + hit) * 100}%")

def access_set(key):
    access_get(key);
    # global miss
    global value_to_set
    mc.set(key, value_to_set)
    # miss += 1

def access_get(key):
    value = mc.get(key)
    global miss, hit
    # print(value, miss, hit)
    # print(value)
    if value is None:
        miss += 1
    else:
        hit += 1

def access_attach(key):
    global miss, hit
    value = mc.get(key)
    if value is None:
        mc.set(key, value_to_set)
        miss += 1
    else:
        hit += 1

def gen_rand_skewed(skewed, size):
    order = -random.uniform(0, 1) * skewed
    exp_ran = math.exp(order)
    rand_num = int(exp_ran * size)
    # return int(rand_num * kBigPrime % size)
    return rand_num

def test_1():
    # 2M
    # mc.flush_all()
    high_freq = 50
    low_freq = 1024 * 100
    test_times = 1024 * 10
    for i in range(test_times):
        key = gen_rand_skewed(10, low_freq)
        # print(key)
        # if key in mp:
        #     access_get(str(key))
        # else:
        #     mp[key] = 1
        access_attach(str(key))

def test_2():
    # 2M
    # mc.flush_all()
    test_times = 1024 * 300
    
    for i in range(test_times // 5):
        key = i + random.randint(0, 10)
        if key in mp:
            access_get(str(key))
        else:
            mp[key] = 1
            access_set(str(key))

def test_3():
    # 2M
    # mc.flush_all()
    high_freq = 50
    low_freq = 1024 * 100
    test_times = 1024 * 100
    for i in range(test_times):
        key = gen_rand_skewed(10, low_freq)
        # print(key)
        # if key in mp:
        #     access_get(str(key))
        # else:
        #     mp[key] = 1
        access_attach(str(key))

def test_trace(file_name):
    with open(file_name, "r") as f:
        idx = 0
        for lines in f.readlines():
            k, r, _, _ = lines.strip().split()
            for i in range(int(r)):
                key = int(k) + i
                # print("!", key)
                access_attach(str(key))
                idx += 1
                if idx % 10000 == 0:
                    report()
                

if __name__ == "__main__":
    mc.flush_all()
    cur_time = time.time()
    # test_trace("traces/OLTP.lis")
    # test_trace("traces/readrandom_5.lis")
    # test_trace("traces/P1.lis")
    test_trace("traces/OLTP.lis")
    # test_3()
    # test_1()
    report()
    end_time = time.time()
    print(f"spend time: {end_time - cur_time}s")
