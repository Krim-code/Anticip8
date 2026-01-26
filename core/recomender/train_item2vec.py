import os, json, random, math
from collections import defaultdict
import redis
from gensim.models import Word2Vec

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

WALKS_PER_NODE = int(os.getenv("WALKS_PER_NODE", "80"))
WALK_LEN = int(os.getenv("WALK_LEN", "18"))
TOPK = int(os.getenv("TOPK", "30"))

VEC_SIZE = int(os.getenv("VEC_SIZE", "64"))
WINDOW = int(os.getenv("WINDOW", "4"))
EPOCHS = int(os.getenv("EPOCHS", "12"))
WORKERS = int(os.getenv("WORKERS", "4"))

TOPK_PREFIX = os.getenv("TOPK_PREFIX", "anticip8:i2v:topk:")

# --- new knobs ---
WEIGHT_SQUASH = os.getenv("WEIGHT_SQUASH", "log")   # log | sqrt | none
RESTART_PROB = float(os.getenv("RESTART_PROB", "0.12"))  # teleport to start node
POPULAR_PENALTY = float(os.getenv("POPULAR_PENALTY", "0.35"))  # downweight mega-popular destinations
MIN_WALK_LEN = int(os.getenv("MIN_WALK_LEN", "4"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def scan(pattern: str):
    return list(r.scan_iter(match=pattern, count=1000))

def node(svc: str, path: str) -> str:
    return f"{svc}::{path}"

def parse_key(prefix: str, k: str):
    parts = k.split(":", 2)
    if len(parts) != 3 or parts[0] != prefix:
        return None
    return parts[1], parts[2]  # svc, from_path

def squash(w: int) -> float:
    if w <= 0:
        return 0.0
    if WEIGHT_SQUASH == "log":
        return math.log1p(w)          # мягко
    if WEIGHT_SQUASH == "sqrt":
        return math.sqrt(w)           # чуть жёстче
    return float(w)                   # none

def weighted_pick(neigh):
    # neigh: list[(n, w)]
    s = sum(w for _, w in neigh)
    if s <= 0:
        return None
    x = random.uniform(0, s)
    acc = 0.0
    for n, w in neigh:
        acc += w
        if acc >= x:
            return n
    return neigh[-1][0]

def build_graph():
    g = defaultdict(list)
    nodes = set()
    in_deg = defaultdict(float)   # для штрафа популярных destinations

    # trans:svc:/path -> hash(/to_path -> count)
    for k in scan("trans:*"):
        parsed = parse_key("trans", k)
        if not parsed:
            continue
        svc, from_path = parsed
        src = node(svc, from_path)
        nodes.add(src)

        h = r.hgetall(k) or {}
        for to_path, cnt in h.items():
            try:
                c = int(cnt)
            except:
                continue
            if c <= 0:
                continue
            dst = node(svc, to_path)
            nodes.add(dst)

            w = squash(c)
            if w > 0:
                g[src].append((dst, w))
                in_deg[dst] += w

    # trans2:src_svc:/path -> hash(dst_svc|dst_path -> count)
    for k in scan("trans2:*"):
        parsed = parse_key("trans2", k)
        if not parsed:
            continue
        src_svc, from_path = parsed
        src = node(src_svc, from_path)
        nodes.add(src)

        h = r.hgetall(k) or {}
        for dst_key, cnt in h.items():
            if "|" not in dst_key:
                continue
            try:
                c = int(cnt)
            except:
                continue
            if c <= 0:
                continue
            dst_svc, dst_path = dst_key.split("|", 1)
            dst = node(dst_svc, dst_path)
            nodes.add(dst)

            w = squash(c)
            if w > 0:
                g[src].append((dst, w))
                in_deg[dst] += w

    # apply popular destination penalty (prevents "hub" collapse)
    if POPULAR_PENALTY > 0:
        # normalize indegree to [0..1]
        if in_deg:
            mx = max(in_deg.values())
            if mx > 0:
                for src in list(g.keys()):
                    new_neigh = []
                    for dst, w in g[src]:
                        hub = in_deg[dst] / mx
                        w2 = w * (1.0 - POPULAR_PENALTY * hub)
                        if w2 > 0:
                            new_neigh.append((dst, w2))
                    g[src] = new_neigh

    return g, list(nodes)

def make_walks(g, nodes):
    walks = []
    nodes_set = set(nodes)

    for start in nodes:
        if start not in g:
            continue

        for _ in range(WALKS_PER_NODE):
            cur = start
            walk = [cur]

            for _ in range(WALK_LEN - 1):
                # restart/teleport: makes embeddings more "next-step" and less drift
                if random.random() < RESTART_PROB:
                    cur = start
                    walk.append(cur)
                    continue

                neigh = g.get(cur)
                if not neigh:
                    break

                nxt = weighted_pick(neigh)
                if not nxt or nxt not in nodes_set:
                    break

                walk.append(nxt)
                cur = nxt

            if len(walk) >= MIN_WALK_LEN:
                walks.append(walk)

    random.shuffle(walks)
    return walks

def save_topk(model, nodes):
    pipe = r.pipeline(transaction=False)
    saved = 0

    for n in nodes:
        if n not in model.wv:
            continue
        sims = model.wv.most_similar(n, topn=TOPK)
        key = TOPK_PREFIX + n
        pipe.set(key, json.dumps([{"item": it, "cos": float(sc)} for it, sc in sims]), ex=3600)
        saved += 1
        if saved % 200 == 0:
            pipe.execute()

    pipe.execute()
    r.set("anticip8:i2v:meta", json.dumps({
        "saved": saved,
        "topk": TOPK,
        "vec_size": VEC_SIZE,
        "window": WINDOW,
        "epochs": EPOCHS,
        "weight_squash": WEIGHT_SQUASH,
        "restart_prob": RESTART_PROB,
        "popular_penalty": POPULAR_PENALTY,
    }), ex=3600)
    print("Saved:", saved)

def main():
    g, nodes = build_graph()
    edges = sum(len(v) for v in g.values())
    print(f"nodes={len(nodes)} edges={edges}")

    walks = make_walks(g, nodes)
    print("walks:", len(walks))
    if len(walks) < 150:
        print("Too few walks. Run locust longer or increase WALKS_PER_NODE.")
        return

    model = Word2Vec(
        sentences=walks,
        vector_size=VEC_SIZE,
        window=WINDOW,
        min_count=1,
        sg=1,
        negative=10,
        epochs=EPOCHS,
        workers=WORKERS,
    )
    save_topk(model, nodes)

if __name__ == "__main__":
    main()
