from minikv.sst import SSTFile


def test_sst_write_from_memtable():
    mem = {
        "k3": "v3",
        "k1": "v1",
        "k2": "v2",
    }

    sst = SSTFile.write_from_memtable("test.sst", mem)

    print("min_key:", sst.min_key)
    print("max_key:", sst.max_key)

    with open("test.sst", "r") as f:
        for line in f:
            print(repr(line))


def test_sst_search():
    mem = {"k3": "v3", "k1": "v1", "k2": "v2"}
    path = "test.sst"

    sst = SSTFile.write_from_memtable(path, mem)

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            print(repr(line))

    print(sst.search("k1"))  # 期望: v1
    print(sst.search("k2"))  # 期望: v2
    print(sst.search("k9"))  # 期望: None


if __name__ == "__main__":
    # test_sst_write_from_memtable()
    test_sst_search()
