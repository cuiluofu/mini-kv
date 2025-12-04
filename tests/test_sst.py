from minikv.sst import SSTFile


def test_sst_write_from_memtable() -> None:
    """
    Verifies that SSTFile.write_from_memtable():
        - Sorts entries lexicographically by key
        - Emits them correctly into the SST file
        - Initializes min_key and max_key metadata
    """
    mem = {
        "k3": "v3",
        "k1": "v1",
        "k2": "v2",
    }

    sst = SSTFile.write_from_memtable("test.sst", mem)

    print("min_key:", sst.min_key)
    print("max_key:", sst.max_key)

    print("\nSST contents:")
    with open("test.sst", "r", encoding="utf-8") as f:
        for line in f:
            print(repr(line))


def test_sst_search() -> None:
    """
    Tests SSTFile.search() behavior:

      - Keys k1 and k2 are expected to be found.
      - Key k9 does not exist and should return None.
      - The test also prints the SST file to demonstrate on-disk layout.
    """
    mem = {"k3": "v3", "k1": "v1", "k2": "v2"}
    path = "test.sst"

    sst = SSTFile.write_from_memtable(path, mem)

    print("\nSST contents:")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            print(repr(line))

    print("\nSearch results:")
    print("k1 ->", sst.search("k1"))  # expected: v1
    print("k2 ->", sst.search("k2"))  # expected: v2
    print("k9 ->", sst.search("k9"))  # expected: None


if __name__ == "__main__":
    # test_sst_write_from_memtable()
    test_sst_search()
