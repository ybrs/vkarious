import argparse, os

def collect(root):
    m = {}
    for d, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(d, f)
            m[os.path.relpath(p, root)] = p
    return m

def diff_files(a, b):
    diffs = []
    off = 0
    with open(a, 'rb') as f1, open(b, 'rb') as f2:
        while True:
            b1 = f1.read(8192)
            b2 = f2.read(8192)
            if not b1 and not b2:
                break
            n = min(len(b1), len(b2))
            i = 0
            while i < n:
                if b1[i] != b2[i]:
                    s = i
                    while i < n and b1[i] != b2[i]:
                        i += 1
                    diffs.append((off + s, i - s))
                else:
                    i += 1
            if len(b1) != len(b2):
                diffs.append((off + n, abs(len(b1) - len(b2))))
            off += n
    return diffs

def main():
    p = argparse.ArgumentParser()
    p.add_argument('left')
    p.add_argument('right')
    a = p.parse_args()
    l = collect(a.left)
    r = collect(a.right)
    added = set(r) - set(l)
    removed = set(l) - set(r)
    shared = set(l) & set(r)
    total = 0
    for f in added:
        sz = os.path.getsize(r[f])
        total += sz
        print(f"{f}: added {sz} bytes")
    for f in removed:
        sz = os.path.getsize(l[f])
        total += sz
        print(f"{f}: removed {sz} bytes")
    for f in shared:
        ds = diff_files(l[f], r[f])
        if ds:
            for off, ln in ds:
                total += ln
                print(f"{f}: {ln} bytes changed at offset {off}")
    print(f"bytes_changed: {total}")

if __name__ == '__main__':
    main()
