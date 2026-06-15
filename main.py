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
MAX_DEPTH = 10    # Maximum path length constraint [cite: 94]
WIKI_BASE_URL = "https://en.wikipedia.org"

# ==========================================
# ADVANCED LOGGING CONFIGURATION
# ==========================================
# Configured to display: Timestamp | Log Level | Thread Name | Message
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Outputs everything straight to standard print output
    ]
)
logger = logging.getLogger("WikiShortestPath")


# ==========================================
# TIMING PERFORMANCE DECORATOR
# ==========================================
def time_function_execution(func):
    """
    A thread-safe decorator that inspects and profiles function execution.
    
    It captures:
      1. Function entry initialization.
      2. Passed runtime argument values (with intelligent formatting).
      3. Normal or exceptional termination states.
      4. High-precision performance duration tracking using time.perf_counter().
    
    Because it only relies on localized variables and a thread-safe logger, 
    it is safe to use across concurrent background threads.
    """
    def wrapper(*args, **kwargs):
        # 1. Format positional parameters for clean logging.
        # If the wrapped function belongs to a class instance method, 'args[0]' will be 'self'.
        # We slice 'args[1:]' to exclude the noisy string representation of 'self' from your console logs.
        callable_args = args[1:] if args else args
        
        # 2. Build readable key-value trace strings if inputs exist
        args_str = f"args={callable_args}" if callable_args else ""
        kwargs_str = f"kwargs={kwargs}" if kwargs else ""
        
        # 3. Join the filtered strings with a comma, ignoring empty fields
        joined_params = ", ".join(filter(None, [args_str, kwargs_str]))

        # 4. Log the initialization telemetry checkpoint right before execution starts
        logger.info(f"==> STARTING: {func.__name__} ({joined_params})")
        
        # 5. Capture the starting performance time counter in fractional seconds
        start_time = time.perf_counter()
        
        try:
            # 6. Execute the original underlying function and hold its return value
            result = func(*args, **kwargs)
            return result
            
        finally:
            # 7. The 'finally' block is guaranteed to execute even if the function raises an Exception.
            # This ensures your program never loses track of ending log diagnostics or runtime measurements.
            end_time = time.perf_counter()
            execution_duration = end_time - start_time
            
            # 8. Log the closing telemetry metrics with precise duration formatted to 4 decimal places
            logger.info(f"<== ENDED: {func.__name__} | Execution Time: {execution_duration:.4f}s")
            
    return wrapper

# ==========================================
# 1. DATABASE MANAGEMENT LAYER
# ==========================================
class DatabaseManager:
    """
    Handles thread-safe SQLite operations using a Thread-Safe Singleton Pattern.

    This class guarantees that only a single instance of DatabaseManager is ever 
    instantiated and used across your entire multi-threaded lifecycle. It prevents 
    file descriptor corruption, coordinates safe table initialization, and logs 
    traversal history analytics using localized thread execution.
    """
    _instance = None
    _lock = threading.Lock()  # Shared class-level reentrant lock used to synchronize thread allocation

    def __new__(cls, *args, **kwargs):
        """
        Implements the Double-Checked Locking Pattern for thread-safe instantiation.
        
        This optimization layout ensures that background worker threads are only 
        blocked by the synchronization lock the very first time the database engine 
        is set up. Subsequent calls fetch the instance instantly without locking overhead.
        """
        # First Check: If an instance already exists, skip the lock completely for raw speed.
        if not cls._instance:
            # Synchronize threads: Block other concurrent threads until the current thread finishes setup.
            with cls._lock:
                # Second Check: Double-check if another thread built the instance while this thread was waiting.
                if not cls._instance:
                    # Allocate heap space and create the single master instance using object base protocols.
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_name: str = DB_NAME):
        """
        Initializes database attributes and tables safely.
        """
        # CRITICAL GUARD CLAUSE: Because Python always invokes __init__ after __new__, 
        # a standard Singleton will re-run its initialization setup every time a user calls DatabaseManager().
        # We flag self._initialized to true to prevent resetting attributes or tables multiple times.
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self.db_name = db_name
        self._init_db()          # Execute structural table creation routes
        self._initialized = True # Lock down initialization permanently

    def _get_connection(self) -> sqlite3.Connection:
        """
        Creates and returns a standalone SQLite connection context.
        
        SQLite connections cannot naturally be shared across concurrent threads. 
        By invoking a fresh .connect() call per operation inside thread contexts, 
        we respect SQLite's threading boundaries cleanly.
        """
        return sqlite3.connect(self.db_name)

    @time_function_execution
    def _init_db(self):
        """Creates the necessary relational database tables if they do not exist."""
        # Open a distinct operational block using a connection context manager (auto-closes on completion)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Table 1: Stores summary meta-records of evaluated paths, lengths, and completion conditions
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
            
            # Table 2: A simple relational event stream logging every single article touched.
            # Used to instantly aggregate global frequency metrics using SQL 'GROUP BY' statements.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS article_hits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_name TEXT
                )
            """)
            conn.commit() # Flush table structures permanently to the local .db binary file

    @time_function_execution
    def save_path(self, start: str, target: str, path: List[str], reached: bool):
        """Saves a fully traversed or aborted path to the database."""
        # Convert the path list array into a single standardized string arrow representation
        path_str = " -> ".join(path)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Insert the summary row tracking this graph sequence execution branch
            cursor.execute(
                "INSERT INTO paths (start_article, target_article, path_string, length, reached_target) VALUES (?, ?, ?, ?, ?)",
                (start, target, path_str, len(path), 1 if reached else 0)
            )
            
            # 2. Deconstruct the path list and log individual article hits into the analytical stream table
            for article in path:
                cursor.execute("INSERT INTO article_hits (article_name) VALUES (?)", (article,))
            
            conn.commit() # Commit all execution operations inside a single secure thread transaction

    @time_function_execution
    def print_top_stats(self):
        """Queries and displays structural performance and usage summaries from the database."""
        # Terminal layout header boundary
        print("\n" + "="*50 + "\n          DASHBOARD & METRICS LOGS          \n" + "="*50)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Analytics Query 1: Top 5 shortest paths that reached their target successfully
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

            # Analytics Query 2: Top 5 article names that appear most frequently across all processed chains
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

Here are the documented and annotated updates for both the WikiCrawler layer and the WikiShortestPathSolver layer.

The docstrings have been enhanced to thoroughly explain the underlying mechanics, and extensive comments have been integrated throughout the execution loops to detail how the thread synchronization, caching, and batching structures work in practice.

2. CRAWLER / NETWORK PARSING LAYER
Python
# ==========================================
# 2. CRAWLER / NETWORK PARSING LAYER
# ==========================================
class WikiCrawler:
    """
    HTTP Networking Engine and DOM Parsing Layer.

    Responsible for managing persistent TCP connections via an isolated requests.Session 
    object, parsing targeted HTML fragments using BeautifulSoup, and implementing a 
    thread-safe, dual-key local memory caching system to minimize unnecessary network 
    roundtrips and enforce crawling politeness.
    """
    def __init__(self):
        # Initialize a persistent Session connection pool. This enables HTTP Keep-Alive,
        # allowing the re-use of underlying TCP sockets across multiple worker threads,
        # drastically reducing SSL handshake overhead times.
        self.session = requests.Session()
        
        # Apply globally conforming User-Agent identification string to ensure requests
        # comply with the Wikimedia security metadata policy.
        self.session.headers.update({
            'User-Agent': 'WikiShortestPathBot/1.0 (contact@example.com) Educational Exercise'
        })
        
        # Shared in-memory cache map holding structure: {lowercase_lookup_key: (canonical_title, [links])}
        self._cache: Dict[str, Tuple[str, List[str]]] = {}
        
        # Synchronization primitive ensuring that only one worker thread can mutate or read
        # from the _cache dictionary instance at any absolute point in time.
        self._cache_lock = threading.Lock()

    @time_function_execution
    def get_clean_title_and_links(self, title_or_url: str) -> Tuple[str, List[str]]:
        """
        Resolves a raw string input into a normalized URL, extracts internal page links, 
        and caches results across worker threads.

        Args:
            title_or_url (str): The raw article text title or absolute Wikipedia link.

        Returns:
            Tuple[str, List[str]]: A tuple containing the definitive page header title 
            and a clean list of target-filtered child article titles.
        """
        # Convert search query variations to uniform casing to ensure cache matching accuracy
        cache_key = title_or_url.strip().lower()

        # Thread-safe read evaluation. Using a context manager ensures the lock is released
        # under all exit paths, preventing thread deadlocks.
        with self._cache_lock:
            if cache_key in self._cache:
                logger.info(f" [CACHE HIT] Fetching links instantly for: '{title_or_url}'")
                return self._cache[cache_key]
            
        # URL Parsing Boundary Strategy: Check if the string needs construction or decoding
        if title_or_url.startswith("http"):
            url = title_or_url
            # Slice trailing URL segment to serve as initial title string token fallback
            parsed_title = title_or_url.split("/wiki/")[-1].replace("_", " ")
            parsed_title = urllib.parse.unquote(parsed_title)
        else:
            # Swap whitespace formatting with standard Wikipedia underscores
            normalized = title_or_url.replace(" ", "_")
            url = f"{WIKI_BASE_URL}/wiki/{normalized}"
            parsed_title = title_or_url

        try:
            # Issue a blocking HTTP GET request wrapped with a strict 5-second failure timeout
            response = self.session.get(url, timeout=5)
            
            # Non-200 Safe Guard: Abort extraction on broken pages or server rate-limits (404/429/403)
            if response.status_code != 200:
                return parsed_title, []
            
            # Hand over raw HTML text stream to the BeautifulSoup tracking architecture
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Pull the official, canonical title text rendered within the H1 heading
            h1_title = soup.find(id="firstHeading")
            if h1_title:
                parsed_title = h1_title.get_text().strip()

            links: List[str] = []
            
            # Narrow DOM parsing scope explicitly to the central text layout structure.
            # This avoids capturing thousands of global noise links inside sidebar panels or footer components.
            body_content = soup.find(id="bodyContent")
            if body_content:
                for anchor in body_content.find_all("a", href=True):
                    href = anchor['href']
                    
                    # Filtering Regex Pattern: Must match an absolute interior path while ignoring namespaces.
                    # Bypasses system meta-directories containing colon symbols (e.g., Category: or File:) or 
                    # fragment identifiers marked with pound symbols (#).
                    if href.startswith("/wiki/") and not re.search(r"[:#]", href):
                        # Extract the trailing page slug and convert into readable plain text title format
                        linked_title = href.split("/wiki/")[-1].replace("_", " ")
                        linked_title = urllib.parse.unquote(linked_title)
                        links.append(linked_title)
            
            # Thread-safe write execution: Update cache registry for subsequent graph sweeps
            with self._cache_lock:
                # 1. Map via the initial transformed request query lookup state
                self._cache[cache_key] = (parsed_title, links)
                
                # 2. Dual-key indexing: Cache under the canonical title as well to catch variations
                self._cache[parsed_title.lower()] = (parsed_title, links)
                
            return parsed_title, links

        except Exception as e:
            # Capture connection drops or unexpected DOM tracking runtime parsing disruptions safely
            logger.error(f"Error crawling '{title_or_url}': {str(e)}")
            return parsed_title, []

# ==========================================
# 3. BFS GRAPH TRAVERSAL LAYER (SOLVER)
# ==========================================
class WikiShortestPathSolver:
    """
    Orchestrates the Breadth-First Search logic across multi-threaded operations
    to construct optimal paths rapidly.
    
    This solver manages the parallel processing batches, ensures search paths are updated,
    and checks path lengths against constraints before adding them to the queue.
    """
    def __init__(self, crawler: WikiCrawler, db: DatabaseManager):
        self.crawler = crawler  # Injected network parsing instance dependency
        self.db = db            # Injected database single transaction entry point

    @time_function_execution
    def find_shortest_path(self, start_query: str, target_query: str) -> Optional[List[str]]:
        """
        Executes a parallelized BFS algorithm to find the absolute shortest connection path.
        """
        # Bootstrap operations: Fetch edge nodes for source and destination immediately
        _, start_links = self.crawler.get_clean_title_and_links(start_query)
        target_title, _ = self.crawler.get_clean_title_and_links(target_query)
        
        # Deduce a readable representation for the source page input parameter matching
        start_title = start_query.split("/wiki/")[-1].replace("_", " ") if start_query.startswith("http") else start_query
        start_title = urllib.parse.unquote(start_title)
        
        logger.info(f"[!] Target normalized to exact Wiki title: '{target_title}'")
        
        # Identity match edge-case verification step
        if start_title.lower() == target_title.lower():
            return [start_title]

        # Double-ended queue tracks traversal state branches. 
        # Stores tuples inside: (current_article_string, cumulative_path_list_sequence)
        bfs_queue = collections.deque()
        
        # High-speed hash set lookup ensures O(1) membership validation tracking,
        # protecting graph branches from entering circular loops.
        visited_nodes: Set[str] = {start_title.lower()}

        # Initialization sweep: Populates the queue layout using layer-1 structural child links
        for link in start_links:
            if link.lower() not in visited_nodes:
                visited_nodes.add(link.lower())
                bfs_queue.append((link, [start_title, link]))

        # Open the multi-threaded management engine context wrapper
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            while bfs_queue:
                
                # Chunk extraction block: Match total active queue capacity up to the MAX_THREADS threshold limit.
                # This guarantees your execution tracks structural breadth accurately per level before dropping.
                batch_size = min(len(bfs_queue), MAX_THREADS)
                current_batch = [bfs_queue.popleft() for _ in range(batch_size)]
                
                # Track outstanding futures mapping back onto the unique node identifier blocks
                futures_map = {}
                for item, path in current_batch:
                    # Immediate search confirmation: Check if the node extracted matches target requirements
                    if item.lower() == target_title.lower():
                        self.db.save_path(start_title, target_title, path, reached=True)
                        return path
                    
                    # Schedule asynchronous request execution onto a background thread worker pool seat
                    future = executor.submit(self.crawler.get_clean_title_and_links, item)
                    futures_map[future] = (item, path)

                # Event-driven listener loop processing returning network results as they become ready
                for future in concurrent.futures.as_completed(futures_map):
                    current_item, current_path = futures_map[future]
                    real_title, child_links = future.result()
                    
                    logger.info(f" -> Current Processing Node: '{real_title}' (Current depth: {len(current_path)})")
                    
                    # Core expansion layer sweep traversing through extracted children paths
                    for child in child_links:
                        # Success verification step: Return and commit path immediately on match
                        if child.lower() == target_title.lower():
                            final_path = current_path + [child]
                            self.db.save_path(start_title, target_title, final_path, reached=True)
                            return final_path
                        
                        # Loop filter guard step
                        if child.lower() not in visited_nodes:
                            next_path = current_path + [child]
                            
                            # CRITICAL FIX / PRUNING GUARDRAIL: Intercept and drop the path calculation
                            # BEFORE wasting computational space or adding items to the memory allocation queue.
                            if len(next_path) > MAX_DEPTH:
                                # Log structural failure states to track dead branches in analytics tables
                                self.db.save_path(start_title, target_title, next_path, reached=False)
                                continue # Prevent queue allocation and drop execution pipeline safely
                                
                            visited_nodes.add(child.lower())
                            bfs_queue.append((child, next_path))
                    
                    # Commit state to database indicating processing completed for this branch level
                    self.db.save_path(start_title, target_title, current_path, reached=False)

        return None


# ==========================================
# 4. EXECUTION LAYER (CLI)
# ==========================================
@time_function_execution
def main():
    parser = argparse.ArgumentParser(description="Find the shortest path between two Wikipedia articles using localized parallel BFS processing.")
    parser.add_argument("start", type=str, nargs='?', default=None, help="Name or full Wikipedia URL of the starting article [cite: 84, 89]")
    parser.add_argument("target", type=str, nargs='?', default=None, help="Name or full Wikipedia URL of the target article [cite: 90]")
    parser.add_argument("--stats", action="store_true", help="Print the database top analytical summary metrics and exit [cite: 99]")
    
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
        print("\n" + "✓ SUCCESS! SHORT_PATH FOUND: " + " ✓")
        for step, name in enumerate(shortest_path, 1):
            print(f"  Step {step}: {name}")
    else:
        print("\n[-] Failed to find a path within the specified limits.")

    # Show metrics dashboard instantly after running [cite: 99]
    db.print_top_stats()

if __name__ == "__main__":
    main()