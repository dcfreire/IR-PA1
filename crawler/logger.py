from multiprocessing import Process

from bs4 import BeautifulSoup
from bs4.element import Comment

import setproctitle


class Logger:
    def __init__(self, n, manager):
        self.queue = manager.Queue()
        self.n = n
        self.start()

    def start(self):
        Process(target=self.worker).start()

    def worker(self):
        setproctitle.setproctitle('python logger')

        count = 0
        while True:
            msg = self.queue.get()

            if msg == "shutdown" or count > self.n:
                return

            self.print_message(*msg)
            count += 1

    @staticmethod
    def tag_visible(element):
        if element.parent.name in [
            "style",
            "script",
            "head",
            "title",
            "meta",
            "[document]",
        ]:
            return False
        if isinstance(element, Comment):
            return False
        return True

    def add_message(self, html, url, timestamp):
        self.queue.put((html, url, timestamp))

    def print_message(self, html, url, timestamp):
        soup = BeautifulSoup(html, "html.parser")
        texts = soup.find_all(text=True)
        visible_texts = filter(Logger.tag_visible, texts)
        visible_text = " ".join(t.strip() for t in visible_texts)
        title = soup.title.string if soup.title else ""
        print(
            f"{{ \"URL\": \"{url}\",\n  \"Title\": \"{title}\",\n  \"Text\": \"{' '.join(visible_text.split()[:20])}\",\n  \"Timestamp\": {timestamp} }}"
        )

    def shutdown(self):
        self.queue.put("shutdown")
