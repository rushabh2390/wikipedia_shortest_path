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
from typing import List, Set, Dict, Optional, Tuple
import urllib.parse
import requests
from bs4 import BeautifulSoup

# ==========================================
# CONSTANTS & CONFIGURATION
# ==========================================
DB_NAME = "wiki.db"
MAX_THREADS = 10  # Maximum concurrent threads allowed 
MAX_DEPTH = 10    # Maximum path length constraint
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
    A thread-safe decorator that inspects and profiles function execution.
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
    Handles thread-safe SQLite operations using a Thread-Safe Singleton Pattern.
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
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS paths (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    def save_path(self, start: str, target: str, path: List[str], reached: bool):
        path_str = " -> ".join(path)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO paths (start_article, target_article, path_string, length, reached_target) VALUES (?, ?, ?, ?, ?)",
                (start, target, path_str, len(path), 1 if reached else 0)
            )
            for article in path:
                cursor.execute("INSERT INTO article_hits (article_name) VALUES (?)", (article,))
            conn.commit()

    @time_function_execution
    def print_top_stats(self):
        print("\n" + "="*50 + "\n          DASHBOARD & METRICS LOGS          \n" + "="*50)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            print("[+] TOP 5 SHORTEST SUCCESSFUL PATHS:")
            cursor.execute("""
                SELECT path_string, length FROM paths 
                WHERE reached_target = 1 
                ORDER BY length ASC 
                LIMIT 5
            """)
            results = cursor.fetchall()
            if not results:
                print("    No successful paths recorded yet.")
            for i, row in enumerate(results, 1):
                print(f"  {i}. [Steps: {row[1]}] {row[0]}")

            print("[+] TOP 5 MOST FREQUENTLY VISITED ARTICLES:")
            cursor.execute("""
                SELECT article_name, COUNT(article_name) as hit_count 
                FROM article_hits 
                GROUP BY article_name 
                ORDER BY hit_count DESC 
                LIMIT 5
            """)
            results = cursor.fetchall()
            if not results:
                print("    No article logs found.")
            for i, row in enumerate(results, 1):
                print(f"  {i}. {row[0]} (Visited {row[1]} times)")
        print("="*50 + "\n")

# ==========================================
# 2. CRAWLER / NETWORK PARSING LAYER
# ==========================================
class WikiCrawler:
    """
    HTTP Networking Engine and DOM Parsing Layer with Category Mapping Cache.
    """
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WikiShortestPathBot/1.0 (contact@example.com) Educational Exercise'
        })
        # Cache structure: {lowercase_key: (canonical_title, [links], [categories])}
        self._cache: Dict[str, Tuple[str, List[str], List[str]]] = {}
        self._cache_lock = threading.Lock()

    @time_function_execution
    def get_clean_title_and_links(self, title_or_url: str) -> Tuple[str, List[str], List[str]]:
        """
        Resolves input query, extracts structural child links, and harvests categories.
        """
        cache_key = title_or_url.strip().lower()
        categories: List[str] = []

        with self._cache_lock:
            if cache_key in self._cache:
                logger.info(f" [CACHE HIT] Fetching metrics instantly for: '{title_or_url}'")
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
            
            # Harvest Categories directly from the DOM footer layout boundary
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
        """
        Computes thematic similarity scores using category intersection tokens.
        """
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
    Orchestrates an optimized, multi-threaded Best-First Search guided 
    by category mapping heuristics to prioritize hyperlinked paths.
    """
    def __init__(self, crawler: WikiCrawler, db: DatabaseManager):
        self.crawler = crawler  
        self.db = db            

    @time_function_execution
    def find_shortest_path(self, start_query: str, target_query: str) -> Optional[List[str]]:
        """
        Executes a parallelized heuristic priority search to identify path configurations.
        """
        # Bootstrap operations: Resolve boundaries and collect core targets
        _, start_links, start_categories = self.crawler.get_clean_title_and_links(start_query)
        target_title, _, target_categories = self.crawler.get_clean_title_and_links(target_query)

        start_title = start_query.split("/wiki/")[-1].replace("_", " ") if start_query.startswith("http") else start_query
        start_title = urllib.parse.unquote(start_title)
        
        logger.info(f"[!] Target normalized to exact Wiki title: '{target_title}'")
        if start_title.lower() == target_title.lower():
            return [start_title]

        # Use PriorityQueue to guide traversal based on weights
        # Queue format: (negative_heuristic_priority, current_path_depth, current_node_name, cumulative_path)
        bfs_queue = queue.PriorityQueue()
        visited_nodes: Set[str] = {start_title.lower()}

        # Populate priority queue weights for initial child layers
        for link in start_links:
            if link.lower() not in visited_nodes:
                visited_nodes.add(link.lower())
                initial_path = [start_title, link]
                
                # Check child elements instantly
                if link.lower() == target_title.lower():
                    self.db.save_path(start_title, target_title, initial_path, reached=True)
                    return initial_path
                
                bfs_queue.put((0, 2, link, initial_path))

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            while not bfs_queue.empty():
                
                # Extract concurrent execution frames based on the thread ceiling
                current_batch = []
                while not bfs_queue.empty() and len(current_batch) < MAX_THREADS:
                    current_batch.append(bfs_queue.get())
                
                futures_map = {}
                for priority, depth, item, path in current_batch:
                    if item.lower() == target_title.lower():
                        self.db.save_path(start_title, target_title, path, reached=True)
                        return path
                    
                    future = executor.submit(self.crawler.get_clean_title_and_links, item)
                    futures_map[future] = (item, path)

                for future in concurrent.futures.as_completed(futures_map):
                    current_item, current_path = futures_map[future]
                    real_title, child_links, child_categories = future.result() # Corrected unpacking signature
                    
                    logger.info(f" -> Current Processing Node: '{real_title}' (Current depth: {len(current_path)})")
                    
                    # Calculate thematic weight for the current children cluster context
                    priority_weight = self.crawler.calculate_heuristic_score(child_categories, target_categories)
                    
                    for child in child_links:
                        if child.lower() == target_title.lower():
                            final_path = current_path + [child]
                            self.db.save_path(start_title, target_title, final_path, reached=True)
                            return final_path
                        
                        if child.lower() not in visited_nodes:
                            next_path = current_path + [child]
                            
                            if len(next_path) > MAX_DEPTH:
                                self.db.save_path(start_title, target_title, next_path, reached=False)
                                continue
                                
                            visited_nodes.add(child.lower())
                            # Higher scores mean closer connection, so negate priority to keep it at the top of the min-heap
                            bfs_queue.put((-priority_weight, len(next_path), child, next_path))
                    
                    self.db.save_path(start_title, target_title, current_path, reached=False)

        return None


# ==========================================
# 4. EXECUTION LAYER (CLI)
# ==========================================
@time_function_execution
def main():
    parser = argparse.ArgumentParser(description="Find the shortest path between two Wikipedia articles using parallel heuristic processing.")
    parser.add_argument("start", type=str, nargs='?', default=None)
    parser.add_argument("target", type=str, nargs='?', default=None)
    parser.add_argument("--stats", action="store_true")
    
    args = parser.parse_args()
    db = DatabaseManager()
    
    if args.stats:
        db.print_top_stats()
        sys.exit(0)

    crawler = WikiCrawler()
    solver = WikiShortestPathSolver(crawler, db)

    print(f"[*] Initiating search tracking from: '{args.start}' -> Target: '{args.target}'")
    shortest_path = solver.find_shortest_path(args.start, args.target)

    if shortest_path:
        print("\n✓ SUCCESS! SHORT_PATH FOUND: ✓")
        for step, name in enumerate(shortest_path, 1):
            print(f"  Step {step}: {name}")
    else:
        print("\n[-] Failed to find a path within the specified limits.")

    db.print_top_stats()

if __name__ == "__main__":
    main()