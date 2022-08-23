from crawler import Crawler
import argparse
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl the web")
    parser.add_argument("-s", required=True, help="Seed file", type=str)
    parser.add_argument("-n", required=True, help="Number of webpages to be crawled", type=int)
    parser.add_argument("-d", action="store_true", help="Run in debug mode")
    args = parser.parse_args()

    with open(args.s, "r") as fp:
        urls = fp.readlines()
    crawler = Crawler((os.cpu_count() or 4) * 2, 40, debug=args.d, directory="warc_store", n=args.n, depth=10)
    crawler.start(
        urls
    )
