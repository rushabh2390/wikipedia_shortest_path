import argparse
import collections
import concurrent.futures
from html import parser
import queue
import re
import sqlite3
import sys
import threading
import time
import logging
import uuid
from typing import List, Set, Dict, Optional, Tuple
import urllib.parse
import requests
from bs4 import BeautifulSoup

# ==========================================
# CONSTANTS & CONFIGURATION
# ==========================================
DB_NAME = "wiki.db"
MAX_THREADS = 10  # Maximum concurrent worker threads allowed in the pool
MAX_DEPTH = 10    # Maximum permitted path link segments before graph branches are pruned
WIKI_BASE_URL = "https://en.wikipedia.org"

# ==========================================
# ADVANCED LOGGING CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("WikiShortestPath")


# ==========================================
# TIMING PERFORMANCE DECORATOR
# ==========================================
def time_function_execution(func):
    """
    A thread-safe profiling decorator that monitors and logs function runtime metrics.
    """
    def wrapper(*args, **kwargs):
        callable_args = args[1:] if args else args
        args_str = f"args={callable_args}" if callable_args else ""
        kwargs_str = f"kwargs={kwargs}" if kwargs else ""
        joined_params = ", ".join(filter(None, [args_str, kwargs_str]))

        logger.info(f"==> STARTING: {func.__name__} ({joined_params})")
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.perf_counter()
            execution_duration = end_time - start_time
            logger.info(f"<== ENDED: {func.__name__} | Execution Time: {execution_duration:.4f}s")
    return wrapper

# ==========================================
# 1. DATABASE MANAGEMENT LAYER
# ==========================================
class DatabaseManager:
    """
    Manages persistent SQLite operations utilizing a Thread-Safe Singleton Pattern.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_name: str = DB_NAME):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.db_name = db_name
        self._init_db()
        self._initialized = True

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_name)

    @time_function_execution
    def _init_db(self):
        """Builds mandatory relational telemetry schema definitions if absent from storage."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # ADDED run_id tracking column across metrics schemas to prevent cross-session pollution
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS paths (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    start_article TEXT,
                    target_article TEXT,
                    path_string TEXT,
                    length INTEGER,
                    reached_target INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS article_hits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    start_article TEXT,
                    target_article TEXT,
                    article_name TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS category_mapping (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_name TEXT,
                    category_name TEXT,
                    UNIQUE(article_name, category_name)
                )
            """)
            conn.commit()

    @time_function_execution
    def save_path(self, run_id: str, start: str, target: str, path: List[str], reached: bool):
        """Commits an evaluated graph traversal route sequence directly into persistent tables."""
        path_str = " -> ".join(path)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO paths (run_id, start_article, target_article, path_string, length, reached_target) VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, start, target, path_str, len(path), 1 if reached else 0)
            )
            conn.commit()

    @time_function_execution
    def log_article_hit(self, run_id: str, start: str, target: str, article: str):
        """Logs an individual worker node interaction linked explicitly to the specific active run session ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO article_hits (run_id, start_article, target_article, article_name) VALUES (?, ?, ?, ?)",
                (run_id, start, target, article)
            )
            conn.commit()

    @time_function_execution
    def print_top_stats(self, run_id: Optional[str] = None, start: Optional[str] = None, target: Optional[str] = None):
        """
        Queries analytics tables to display system usage metrics.
        Filters telemetry records strictly by the active run token to isolate data cleanly.
        """
        print("\n" + "="*50 + "\n          DASHBOARD & METRICS LOGS          \n" + "="*50)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if run_id and start and target:
                print(f"[+] TOP 5 SHORTEST SUCCESSFUL PATHS FOR THIS RUN: '{start}' -> '{target}':")
                path_query = """
                    SELECT path_string, length FROM paths 
                    WHERE reached_target = 1 
                      AND run_id = ?
                    ORDER BY length ASC 
                    LIMIT 5
                """
                cursor.execute(path_query, (run_id,))
            else:
                print("[+] TOP 5 SHORTEST SUCCESSFUL PATHS GLOBAL:")
                path_query = """
                    SELECT path_string, length FROM paths 
                    WHERE reached_target = 1 
                    ORDER BY length ASC 
                    LIMIT 5
                """
                cursor.execute(path_query)
                
            results = cursor.fetchall()
            if not results:
                print("    No successful paths recorded yet for this session.")
            for i, row in enumerate(results, 1):
                link_transitions = row[1] - 1
                print(f"  {i}. [Path Length (Links Crossed): {link_transitions}] {row[0]}")

            if run_id and start and target:
                print(f"\n[+] TOP 5 MOST FREQUENTLY VISITED ARTICLES FOR THIS RUN:")
                hit_query = """
                    SELECT article_name, COUNT(article_name) as hit_count 
                    FROM article_hits 
                    WHERE run_id = ?
                    GROUP BY article_name 
                    ORDER BY hit_count DESC, article_name ASC
                    LIMIT 5
                """
                cursor.execute(hit_query, (run_id,))
            else:
                print("\n[+] TOP 5 MOST FREQUENTLY VISITED ARTICLES GLOBAL:")
                hit_query = """
                    SELECT article_name, COUNT(article_name) as hit_count 
                    FROM article_hits 
                    GROUP BY article_name 
                    ORDER BY hit_count DESC, article_name ASC
                    LIMIT 5
                """
                cursor.execute(hit_query)
                
            results = cursor.fetchall()
            if not results:
                print("    No article logs found for this search configuration.")
            for i, row in enumerate(results, 1):
                print(f"  {i}. {row[0]} (Visited {row[1]} times)")
        print("="*50 + "\n")

# ==========================================
# 2. CRAWLER / NETWORK PARSING LAYER
# ==========================================
class WikiCrawler:
    """
    HTTP Networking Engine and Document Object Model (DOM) Parsing Architecture.
    """
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WikiShortestPathBot/1.0 (contact@example.com) Educational Exercise'
        })
        self._cache: Dict[str, Tuple[str, List[str], List[str]]] = {}
        self._cache_lock = threading.Lock()

    @time_function_execution
    def get_clean_title_and_links(self, title_or_url: str, stop_signal: Optional[threading.Event] = None) -> Tuple[str, List[str], List[str]]:
        """Resolves a search token or URL, processes structural HTML markup, and records category taxonomies."""
        if stop_signal and stop_signal.is_set():
            return "", [], []

        cache_key = title_or_url.strip().lower()
        categories: List[str] = []

        with self._cache_lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
            
        if title_or_url.startswith("http"):
            url = title_or_url
            parsed_title = title_or_url.split("/wiki/")[-1].replace("_", " ")
            parsed_title = urllib.parse.unquote(parsed_title)
        else:
            normalized = title_or_url.replace(" ", "_")
            url = f"{WIKI_BASE_URL}/wiki/{normalized}"
            parsed_title = title_or_url

        try:
            if stop_signal and stop_signal.is_set():
                return "", [], []

            response = self.session.get(url, timeout=5)
            if response.status_code != 200:
                return parsed_title, [], []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            h1_title = soup.find(id="firstHeading")
            if h1_title:
                parsed_title = h1_title.get_text().strip()

            links: List[str] = []
            body_content = soup.find(id="bodyContent")
            if body_content:
                for anchor in body_content.find_all("a", href=True):
                    href = anchor['href']
                    if href.startswith("/wiki/") and not re.search(r"[:#]", href):
                        linked_title = href.split("/wiki/")[-1].replace("_", " ")
                        linked_title = urllib.parse.unquote(linked_title)
                        links.append(linked_title)
            
            cat_container = soup.find(id="mw-normal-catlinks")
            if cat_container:
                for cat_link in cat_container.find_all("a", href=True):
                    if "Category:" in cat_link['href']:
                        clean_category = cat_link.get_text().strip()
                        categories.append(clean_category)

            with self._cache_lock:
                self._cache[cache_key] = (parsed_title, links, categories)
                self._cache[parsed_title.lower()] = (parsed_title, links, categories)
            
            return parsed_title, links, categories

        except Exception as e:
            logger.error(f"Error crawling '{title_or_url}': {str(e)}")
            return parsed_title, [], []
    
    @staticmethod
    def calculate_heuristic_score(node_categories: List[str], target_categories: List[str]) -> int:
        """Calculates a thematic proximity score between two category arrays using intersection sets."""
        node_set = set(cat.lower() for cat in node_categories)
        target_set = set(cat.lower() for cat in target_categories)
        
        exact_matches = len(node_set.intersection(target_set))
        
        node_tokens = set(" ".join(node_set).split())
        target_tokens = set(" ".join(target_set).split())
        token_matches = len(node_tokens.intersection(target_tokens))
        
        return (exact_matches * 10) + token_matches

# ==========================================
# 3. HEURISTIC PRIORITY GRAPH SOLVER
# ==========================================
class WikiShortestPathSolver:
    """
    Orchestrates an optimized multi-threaded Best-First Search guided strictly by category weights.
    """
    def __init__(self, crawler: WikiCrawler, db: DatabaseManager):
        self.crawler = crawler  
        self.db = db            

    @time_function_execution
    def find_shortest_path(self, start_query: str, target_query: str, run_id: str) -> Optional[List[str]]:
        """Executes parallelized category heuristic tracking to discover shortest path sequences."""
        _, start_links, start_categories = self.crawler.get_clean_title_and_links(start_query)
        target_title, _, target_categories = self.crawler.get_clean_title_and_links(target_query)

        start_title = start_query.split("/wiki/")[-1].replace("_", " ") if start_query.startswith("http") else start_query
        start_title = urllib.parse.unquote(start_title)
        
        logger.info(f"[!] Target normalized to exact Wiki title: '{target_title}'")
        if start_title.lower() == target_title.lower():
            return [start_title]

        self.db.log_article_hit(run_id, start_title, target_title, start_title)

        bfs_queue = queue.PriorityQueue()
        visited_lock = threading.Lock()
        
        # ADDED write lock to handle data logging strictly inside multi-threaded racing segments
        db_write_lock = threading.Lock() 
        visited_nodes: Set[str] = {start_title.lower()}

        stop_signal = threading.Event()

        logger.info("[*] Pre-scoring initial boundary layer links based on Category heuristic...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures_map = {
                executor.submit(self.crawler.get_clean_title_and_links, link, stop_signal): link 
                for link in start_links[:60] 
            }
            
            for future in concurrent.futures.as_completed(futures_map):
                link = futures_map[future]
                real_child_title, _, child_categories = future.result()
                if not real_child_title:
                    continue
                
                if real_child_title.lower() == target_title.lower():
                    final_path = [start_title, real_child_title]
                    with db_write_lock:
                        if not stop_signal.is_set():
                            stop_signal.set()
                            self.db.log_article_hit(run_id, start_title, target_title, real_child_title)
                            self.db.save_path(run_id, start_title, target_title, final_path, reached=True)
                            return final_path
                
                priority_weight = self.crawler.calculate_heuristic_score(child_categories, target_categories)
                bfs_queue.put((-priority_weight, 2, real_child_title, [start_title, real_child_title]))

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            while not bfs_queue.empty() and not stop_signal.is_set():
                
                current_batch = []
                while not bfs_queue.empty() and len(current_batch) < MAX_THREADS:
                    current_batch.append(bfs_queue.get())
                
                futures_map = {}
                for priority, depth, item, path in current_batch:
                    if stop_signal.is_set():
                        break
                    if item.lower() == target_title.lower():
                        with db_write_lock:
                            if not stop_signal.is_set():
                                stop_signal.set()
                                self.db.log_article_hit(run_id, start_title, target_title, item)
                                self.db.save_path(run_id, start_title, target_title, path, reached=True)
                                return path
                    
                    future = executor.submit(self.crawler.get_clean_title_and_links, item, stop_signal)
                    futures_map[future] = (item, path)

                for future in concurrent.futures.as_completed(futures_map):
                    if stop_signal.is_set():
                        break
                        
                    current_item, current_path = futures_map[future]
                    real_title, child_links, child_categories = future.result()
                    
                    if not real_title and not child_links:
                        continue

                    with db_write_lock:
                        if not stop_signal.is_set():
                            self.db.log_article_hit(run_id, start_title, target_title, real_title)
                    
                    logger.info(f" -> Current Processing Node: '{real_title}' (Path Length: {len(current_path)-1})")
                    
                    for child in child_links:
                        if stop_signal.is_set():
                            return None

                        if child.lower() == target_title.lower():
                            final_path = current_path + [child]
                            # FIXED: Wrapped target logging inside a lock query to drop concurrent duplicates instantly
                            with db_write_lock:
                                if not stop_signal.is_set():
                                    stop_signal.set()
                                    self.db.log_article_hit(run_id, start_title, target_title, child)
                                    self.db.save_path(run_id, start_title, target_title, final_path, reached=True)
                                    return final_path
                        
                        with visited_lock:
                            if child.lower() not in visited_nodes and not stop_signal.is_set():
                                visited_nodes.add(child.lower())
                                next_path = current_path + [child]
                                
                                if len(next_path) > MAX_DEPTH:
                                    with db_write_lock:
                                        self.db.save_path(run_id, start_title, target_title, next_path, reached=False)
                                    continue
                                
                                priority_weight = self.crawler.calculate_heuristic_score(child_categories, target_categories)
                                bfs_queue.put((-priority_weight, len(next_path), child, next_path))
                    
                    with db_write_lock:
                        self.db.save_path(run_id, start_title, target_title, current_path, reached=False)

        return None

# ==========================================
# 4. EXECUTION LAYER (CLI)
# ==========================================
@time_function_execution
def main():
    parser = argparse.ArgumentParser(description="Find the shortest path between two Wikipedia articles using parallel heuristic processing.")
    parser.add_argument("start", type=str, nargs='?', default=None, help="Name or absolute URL of the origin article page.")
    parser.add_argument("target", type=str, nargs='?', default=None, help="Name or absolute URL of the objective target article page.")
    parser.add_argument("--stats", action="store_true", help="Queries and reports localized summary metrics logs directly from storage tables.")
    
    args = parser.parse_args()
    db = DatabaseManager()
    
    if args.stats:
        db.print_top_stats()
        sys.exit(0)

    if not args.start or not args.target:
        parser.print_help()
        print("\n❌ Error: Missing required search terms. Supply 'start' and 'target' parameters or run with --stats.")
        sys.exit(1)

    # Generated a unique session execution token for this specific run
    run_id = str(uuid.uuid4())

    crawler = WikiCrawler()
    solver = WikiShortestPathSolver(crawler, db)

    print(f"[*] Initiating search tracking from: '{args.start}' -> Target: '{args.target}'")
    shortest_path = solver.find_shortest_path(args.start, args.target, run_id)

    if shortest_path:
        print("\n✓ SUCCESS! SHORT_PATH FOUND: ✓")
        for step, name in enumerate(shortest_path, 1):
            print(f"  Step {step}: {name}")
    else:
        print("\n[-] Failed to find a path within the specified limits.")

    # Isolation parameters applied cleanly
    db.print_top_stats(run_id=run_id, start=args.start, target=args.target)

if __name__ == "__main__":
    main()