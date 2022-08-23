import os
from multiprocessing import Process

from requests import Response
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter


class Data:
    def __init__(self, data):
        self.data = data
        self.cp = 0

    def read(self, size):
        ret = self.data[self.cp : self.cp + size]
        self.cp = self.cp + size
        return ret


class WARCworker:
    def __init__(self, directory, nrecords, manager):
        self.queue = manager.Queue()
        self.directory = directory
        self.nrecords = nrecords
        self.count = manager.Value("i", 0)
        self.start()

    def start(self):
        Process(target=self.worker).start()

    def worker(self):

        self.url_history = set()
        self.current_count = 1
        self.current_file_name = 1
        self.current_file = open(
            os.path.join(self.directory, str(self.current_file_name) + ".warc.gz"), "wb"
        )
        self.writer = WARCWriter(self.current_file, gzip=True)
        while self.count.value < self.nrecords:
            resp: Response = self.queue.get()
            if resp.url in self.url_history:
                continue

            self.url_history.add(resp.url)

            if resp == "shutdown":
                self.current_file.close()
                return

            if self.current_count > 1000:
                self.current_file_name += 1
                self.current_file.close()
                self.current_file = open(
                    os.path.join(
                        self.directory, str(self.current_file_name) + ".warc.gz"
                    ),
                    "wb",
                )
                self.writer = WARCWriter(self.current_file, gzip=True)
                self.current_count = 1
            try:
                self.write(resp)
                self.count.value += 1
                self.current_count += 1

            except Exception as e:
                print(e)
                print(e.__class__)

    def add(self, resp):
        self.queue.put(resp, block=False)

    def write(self, resp):
        headers_list = resp.headers.items()
        http_headers = StatusAndHeaders("200 OK", headers_list, protocol="HTTP/1.0")
        content = Data(resp.content)
        record = self.writer.create_warc_record(
            resp.url, "response", payload=content, http_headers=http_headers
        )
        self.writer.write_record(record)

    def shutdown(self):
        self.queue.put("shutdown")
