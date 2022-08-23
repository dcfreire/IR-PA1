from multiprocessing import Queue
from time import sleep, time
from urllib.parse import urlparse

from reppy.robots import Robots, AllowAll
from url_normalize import url_normalize

class Root:
    def __init__(self, robots):
        self.count = 1
        self.robots = robots
        self.last_visit = time()

    def visit(self):
        self.last_visit = time()

    def count_up(self):
        self.count += 1

    def get_delay(self):
        try:
            return self.robots.delay
        except AttributeError:
            return None

    def __gt__(self, other):
        return self.count > other

    def __lt__(self, other):
        return self.count < other

    def __ge__(self, other):
        return self.count >= other

    def __le__(self, other):
        return self.count <= other


class Frontier:
    def __init__(self, depth) -> None:
        self.depth = depth
        self.url_history = set()
        self.roots = {}
        self.frontier = Queue(maxsize=5000)


    def enqueue(self, url):

        root = url_normalize(urlparse(url).hostname) or ""

        if self.roots.get(root, 0) > self.depth:
            self.roots[root].robots = None
            return


        if root in self.roots:
            self.roots[root].count_up()
        else:

            try:
                robots_url = Robots.robots_url(root)
                robots = Robots.fetch(robots_url)
            except:
                robots = AllowAll(root)

            self.roots[root] = Root(robots)
            self.frontier.put(root)

        if url not in self.url_history and self.is_allowed(root):
            self.url_history.add(url)
            self.frontier.put(url)


    def get(self):
        return self.frontier.get()

    def wait_politely(self, url):
        try:
            sleep_time = (self.roots[url].get_delay() or 0.1) - (time() - self.roots[url].last_visit)
            sleep(sleep_time)
            self.roots[url].visit()
        except ValueError:
            pass
        except Exception as e:
            print(e)
            print(e.__class__)

    def is_allowed(self, url):
        return self.roots[url].robots.allowed(url, "CarneiroBot")
