from minikv import MiniKV,MiniKVConfig,WriteMode

def basic_test():
    cfg = MiniKVConfig()
    kv = MiniKV(cfg)

    kv.open()

    # 1. 未 open 就操作，应当报错
    # 2. open 后 put/get
    kv.put("123","qtf")
    kv.put("456","hello")
    v1 = kv.get("123")
    v2 = kv.get("456")
    v3 = kv.get("789")

    print("123 ->", v1)
    print("456 ->", v2)
    print("789 ->", v3)
    # 3. 覆盖写
    kv.put("123","new_qtf")
    print("123 after overwrite ->", kv.get("123"))

    # 4. delete 后 get 返回 None
    kv.delete("123")
    print("123 after delete ->", kv.get("123"))

    # 5. close 后再次 put/get 应当报错
    kv.close()
    try:
        kv.put("999","x")
    except RuntimeError as e:
        print("put after close error:" ,e)

if __name__ == "__main__":
    basic_test()