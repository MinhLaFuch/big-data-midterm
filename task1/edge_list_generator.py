import numpy as np

vertices = 100_000
edges = 1_000_000_000
chunk = 10_000_000

with open("edges.txt", "w") as f:
    written = 0
    while written < edges:
        n = min(chunk, edges - written)
        u = np.random.randint(0, vertices, size=n)
        v = np.random.randint(0, vertices, size=n)
        mask = u == v
        v[mask] = (v[mask] + 1) % vertices  # fix self-loops
        np.savetxt(f, np.stack([u, v], axis=1), fmt="%d", delimiter=" ")
        written += n
        print(f"{written:,} / {edges:,}")