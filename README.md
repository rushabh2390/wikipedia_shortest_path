# Wikipedia Shortest Path Finder

A high-performance, multi-threaded command-line interface (CLI) application built to discover the shortest path between any two Wikipedia articles using an optimized parallel Breadth-First Search (BFS) graph traversal strategy. 

The application utilizes persistent SQLite transaction tables to track structural performance analytics, maps concurrent network handshakes via an isolated thread pool, and guarantees graceful execution using pre-queue graph pruning limits.

---

## 🛠 Key Structural Highlights & Features

1. **Parallelized Graph Traversal (Multi-threaded BFS)**
   * Uses a thread pool executor (`concurrent.futures.ThreadPoolExecutor`) limited strictly to `10` concurrent workers to parse batches of child links simultaneously.
2. **Pre-Queue Depth Guardrails**
   * Implements automated path interception. If a path sequence exceeds the `MAX_DEPTH` boundary of `10` links, the branch is logged directly to the analytics table as unreached and pruned immediately before hitting the memory allocation queue. This completely eliminates exponential memory explosion.
3. **Robust Network & Parsing Architecture**
   * Manages persistent TCP connections via an isolated `requests.Session` pipeline with structured user-agent compliance headers.
   * Utilizes a thread-safe, dual-key local memory caching system protected by mutual exclusion locks (`threading.Lock`) to prevent redundant networking requests.
4. **Thread-Safe Persistent Data Storage**
   * Implements a **Thread-Safe Singleton Pattern** database manager powered by double-checked validation locking to route cross-thread path states safely into a local `wiki.db` file.
5. **Granular Telemetry Decorators**
   * Every structural processing transaction layer uses thread-safe performance decorators utilizing `time.perf_counter()` to trace computational durations, thread allocations, and invocation state parameters directly into standard log output streams.

---

## 📊 Database Schema Analytics

Every execution cycle captures data within a local `wiki.db` instance across two primary tables:
* **`paths`**: Logs the individual traversal arrays (`start_article`, `target_article`, `path_string`), total depth length, precise completion timestamps, and a conditional binary status (`reached_target = 1` or `0`).
* **`article_hits`**: Automatically registers every unique structural text article encountered across processed chains to compute general network density models.

---

## 🚀 Prerequisites & Installation
### 1. Install via requirements.txt (Alternative)
If you have a `requirements.txt` file configured in your project directory, you can install the required dependencies all at once:

```bash
pip install -r requirements.txt
```

### 2. Run script with below command where we pass start and end target.

```bash
python main.py "Walton Cardiff" "Bristol"
```

### 3. Accessing the Statistical Dashboard.
To pull historical metrics from the SQLite analytics engine without initializing a network scraping traversal route, supply the --stats flag:

```bash
python main.py --stats
```

#### 4 Sample Output Logs.
```
2026-06-15 13:00:01,123 [INFO] (MainThread) ==> STARTING: main ()
[*] Initiating search tracking from: 'Walton Cardiff' -> Target: 'Bristol'
2026-06-15 13:00:01,125 [INFO] (MainThread) ==> STARTING: find_shortest_path (args=('Walton Cardiff', 'Bristol'))
[!] Target normalized to exact Wiki title: 'Bristol'
[*] Queue successfully seeded with 42 links. Spawning worker threads...
2026-06-15 13:00:01,842 [INFO] (ThreadPoolExecutor-0_0) -> Current Processing Node: 'Gloucestershire' (Current depth: 2)
2026-06-15 13:00:02,105 [INFO] (MainThread) <== ENDED: find_shortest_path | Execution Time: 0.9802s

✓ SUCCESS! SHORT_PATH FOUND:  ✓
  Step 1: Walton Cardiff
  Step 2: Gloucestershire
  Step 3: Bristol

==================================================
          DASHBOARD & METRICS LOGS          
==================================================
[+] TOP 5 SHORTEST SUCCESSFUL PATHS:
  1. [Steps: 3] Walton Cardiff -> Gloucestershire -> Bristol
[+] TOP 5 MOST FREQUENTLY VISITED ARTICLES:
  1. Gloucestershire (Visited 1 times)
  2. Walton Cardiff (Visited 1 times)
  3. Bristol (Visited 1 times)
==================================================

2026-06-15 13:00:02,110 [INFO] (MainThread) <== ENDED: main | Execution Time: 0.9870s

```