from minikv import MiniKV, MiniKVConfig, WriteMode


def basic_test() -> None:
    """
    Runs a basic end-to-end sanity check for the MiniKV engine.

    Coverage:
      - Opening the engine
      - Basic put/get semantics
      - Overwrite behavior
      - Logical deletion
      - Safety checks after close()
    """
    cfg = MiniKVConfig()
    kv = MiniKV(cfg)

    kv.open()

    # 1. Basic put/get operations after open().
    kv.put("123", "qtf")
    kv.put("456", "hello")
    v1 = kv.get("123")
    v2 = kv.get("456")
    v3 = kv.get("789")  # Non-existent key

    print("123 ->", v1)
    print("456 ->", v2)
    print("789 ->", v3)

    # 2. Overwrite an existing key and verify the latest value is visible.
    kv.put("123", "new_qtf")
    print("123 after overwrite ->", kv.get("123"))

    # 3. Delete a key and verify that subsequent reads return None.
    kv.delete("123")
    print("123 after delete ->", kv.get("123"))

    # 4. After close(), any further writes should raise a RuntimeError.
    kv.close()
    try:
        kv.put("999", "x")
    except RuntimeError as e:
        print("put after close error:", e)


if __name__ == "__main__":
    basic_test()
