import time
from multiprocessing.pool import ThreadPool, Pool
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from url_normalize import url_normalize

from .frontier import Frontier
from .logger import Logger
from .manager import FrontierManager
from .warc import WARCworker


class Crawler:
    def __init__(
        self,
        processes: int,
        threads: int,
        debug: bool = False,
        directory: str = "",
        n: int = 10**5,
        depth: int = 100,
    ):

        self.directory = directory
        self.depth: int = depth
        self.processes: int = processes
        self.threads: int = threads
        self.debug: bool = debug
        self.n: int = n

    def shutdown(self):
        if self.debug:
            self.logger.shutdown()
        self.warc.shutdown()

    def start(self, urls: list):
        with FrontierManager() as manager:
            self.frontier: Frontier = manager.Frontier(self.depth)
            self.warc = WARCworker(self.directory, self.n, manager)
            self.child_results = manager.Queue()

            if self.debug:
                self.logger = Logger(self.n, manager)

            for url in urls:
                self.frontier.enqueue(url_normalize(url))

            with Pool(processes=self.processes, maxtasksperchild=1) as pool:
                pool.map_async(self.worker, range(self.processes))
                while True:
                    res = self.child_results.get()
                    if res == "SHUTDOWN":
                        self.shutdown()
                        return

                    pool.apply_async(self.worker, (res,))


    def worker(self, workerid: int):
        self.workerid = workerid
        executed_tasks = 0
        with ThreadPool(processes=self.threads) as pool:
            while True:
                url: str = self.frontier.get()

                if self.warc.count.value >= self.n:
                    self.child_results.put("SHUTDOWN")
                    break

                if executed_tasks >= 1000:
                    time.sleep(5)
                    pool.close()
                    pool.terminate()
                    break

                pool.apply_async(self.request, (url,))
                executed_tasks += 1

        self.child_results.put(workerid)
        return 0

    def request(self, url: str):
        try:
            root = url_normalize(urlparse(url).hostname) or ""
            self.frontier.wait_politely(root)
            head = requests.head(url, timeout=3)
            if 200 <= head.status_code < 400:
                if "text/html" in head.headers["content-type"]:
                    self.frontier.wait_politely(root)
                    resp = requests.get(url, timeout=3)

                    if self.debug:
                        self.logger.add_message(resp.text, url, int(time.time()))

                    self.warc.add(resp)
                    self.parse(resp.text, root)

                else:
                    return
            else:
                return

        except Exception:
            return

    def parse(self, html: str, root: str):
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]

            if href[0] in "#?":
                continue

            if href[0] == "/":
                url = url_normalize(root + href)
            else:
                url = url_normalize(href)

            self.frontier.enqueue(url)
