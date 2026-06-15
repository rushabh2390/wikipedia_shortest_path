# Wikipedia Shortest Path Finder

A high-performance, multi-threaded command-line interface (CLI) application built to discover the shortest path between any two Wikipedia articles. 

Instead of an unguided graph traversal, this engine utilizes an optimized **Category-Heuristic Best-First Search (BFS)** strategy. It evaluates semantic proximity weights based on Wikipedia category overlapping sets to drastically reduce look-ahead network overhead and bypass noisy, unrelated paths.

---

## 🛠 Key Structural Highlights & Features

1. **Heuristic Category Guidance Engine**
   * Pre-scores and prioritizes links using a semantic token intersection matrix computed from Wikipedia's native category metadata. This allows the search to quickly latch onto semantic paths (e.g., navigating through digital archivers to find computer software) while naturally discarding alphabetical noise.
2. **Parallelized Graph Traversal**
   * Employs a thread pool executor (`concurrent.futures.ThreadPoolExecutor`) limited strictly to `10` concurrent worker threads to pull, process, and scrape batches of child links simultaneously.
3. **Multi-Threaded Race-Condition Guardrails**
   * Implements mutual exclusion locks (`threading.Lock`) across state repositories and introduces a database transaction lock (`db_write_lock`) to handle fast multi-threaded racing segments. When a thread discovers the destination milestone, trailing writes are dropped instantly.
4. **Session Isolation Architecture**
   * Assigns a unique cryptographically secure session execution token (`UUIDv4`) to every search run. Telemetry records are completely isolated by session identifiers, preventing historical cross-run pollution from distorting the current execution dashboard.
5. **Robust Network Handshaking & Pruning**
   * Manages persistent TCP connections via an isolated `requests.Session` pipeline with structured user-agent compliance headers.
   * Utilizes a thread-safe, dual-key local memory caching layer to eliminate redundant network hits.
   * Implements automated depth guardrails to safely prune path sequences that exceed the `MAX_DEPTH` boundary of `10` links.

---

## 📊 Database Schema Analytics

Every execution cycle captures telemetry data within a local `wiki.db` instance across the following structured schemas:
* **`paths`**: Logs individual traversal paths (`start_article`, `target_article`, `path_string`), path lengths, precise execution completion timestamps, a conditional target validation binary flag (`reached_target = 1` or `0`), and the isolated `run_id`.
* **`article_hits`**: Registers every unique structural text article encountered and *explicitly fully processed* by worker threads to compute accurate graph density models.
* **`category_mapping`**: Maps and stores unique article-to-category associations locally to enhance performance.

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
2026-06-15 22:34:08,110 [INFO] (MainThread) ==> STARTING: main ()
[*] Initiating search tracking from: 'Walton Cardiff' -> Target: 'Bristol'
2026-06-15 22:34:08,125 [INFO] (MainThread) ==> STARTING: find_shortest_path (args=('Walton Cardiff', 'Bristol', 'bde890f9-24b7-4f05-98f4-aabfb3cdb23b'))
[!] Target normalized to exact Wiki title: 'Bristol'
[*] Pre-scoring initial boundary layer links based on Category heuristic...
2026-06-15 22:34:09,102 [INFO] (ThreadPoolExecutor-0_1) -> Current Processing Node: 'Gloucestershire' (Path Length: 1)
2026-06-15 22:34:09,610 [INFO] (MainThread) <== ENDED: find_shortest_path | Execution Time: 1.4850s

✓ SUCCESS! SHORT_PATH FOUND: ✓
  Step 1: Walton Cardiff
  Step 2: Gloucestershire
  Step 3: Bristol

==================================================
          DASHBOARD & METRICS LOGS          
==================================================
[+] TOP 5 SHORTEST SUCCESSFUL PATHS FOR THIS RUN: 'Walton Cardiff' -> 'Bristol':
  1. [Path Length (Links Crossed): 2] Walton Cardiff -> Gloucestershire -> Bristol

[+] TOP 5 MOST FREQUENTLY VISITED ARTICLES FOR THIS RUN:
  1. Bristol (Visited 1 times)
  2. Gloucestershire (Visited 1 times)
  3. Walton Cardiff (Visited 1 times)
==================================================

2026-06-15 22:34:09,614 [INFO] (MainThread) <== ENDED: main | Execution Time: 1.5040s

```