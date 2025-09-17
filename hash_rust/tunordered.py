from blake3 import blake3

def multiset_hash(rows):
    MOD = 2**128
    acc = 0
    for row in rows:
        # serialize the row deterministically
        data = repr(row).encode("utf-8")
        h = int.from_bytes(blake3(data).digest(length=16), "big")  # 128-bit row hash
        acc = (acc + h) % MOD
    # final digest as hex
    return f"{acc:032x}"

rows1 = [
    (1, "Alice", 100),
    (2, "Bob", 200),
    (3, "Charlie", 300),
]

rows2 = [
    (3, "Charlie", 300),
    (1, "Alice", 100),
    (2, "Bob", 200),
]

print(multiset_hash(rows1))
print(multiset_hash(rows2))
