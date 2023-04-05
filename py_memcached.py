import memcache
import random
# 127.0.0.1 代表服务器IP地址，因为我们得memcached安装在本地，所以其IP地址为127.0.0.1
# 11211 代表端口号，memcached默认端口号为 11211
# debug=True 这个表示开启调试模式
mc = memcache.Client(["127.0.0.1:11211"],debug=True)
# set() 第1个值为key[必须];第2个值为value[必须];第三个值为存活时间[可选]，默认为3小时;关于set更多参数可点击set进去查看

miss = 0
hit = 0

def access_set(key):
    mc.set(key, "value", time=120)

def access_get(key):
    value = mc.get(key)
    if value is None:
        global miss
        miss += 1
    else:
        global hit
        hit += 1
    
if __name__ == "__main__":
    mc.flush_all()
    high_freq = 50
    low_freq = 1024 * 1000
    test_times = 1024 * 100

    for i in range(test_times):
        if (random.randint(0, 2) == 0):
            key = random.randint(0, high_freq)
        else:
            key = random.randint(0, low_freq)
        if (random.randint(0, 2) == 0):
            access_get(str(key))
        else:
            access_set(str(key))
    
    print(f"hit ratio: {hit / (miss + hit) * 100}%")
# for k in range(100):
#     for i in range(50):
#         mc.set(f"username_{i}", "value", time=120)

# for i in range(1024 * 100):
#     mc.set(f"username_{i}", "value", time=120)
#     if (i % (1024 * 20) == 0):
#         for j in range(50):
#             mc.set(f"username_{j}", "value", time=120)

# for i in range(50):
#     value = mc.get(f"username_{i}")
#     if value is None and i < 50:
#         print(f"username_{i} evicted")
