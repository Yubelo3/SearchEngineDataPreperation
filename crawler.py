from page_parser import PageParser
from collections import deque
from page import Page
from typing import List, Tuple
import numpy as np
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import threading
import json
import os
import re
from page_rank import PageRank
from queue import Queue


class Crawler(object):
    def __init__(self,initial_url,max_pages=300, dump_dir="page_data") -> None:
        self.parser = PageParser()
        self.url_queue=Queue()
        self.page_to_id = {}
        self.lock=threading.Lock()
        self.pages=[]
        self.initial_url=initial_url
        self.max_pages=max_pages
        self.dump_dir=dump_dir
        self.connectivity_matrix = None
        self.bar=None

    def crawl(self,url:str,parent_id:int):
        page = self.parser.extract_webpage(url)
        title, last_modified, links, original_page,size = page["title"], page[
            "last_modified"], page["links"], page["original_page"],page["size"]

        with self.lock:
            num_crawled=len(self.pages)
            self.page_to_id[url] = num_crawled
            page_id = num_crawled
            self.pages.append(Page(
                id=num_crawled,
                title=title,
                url=url,
                last_modified=last_modified,
                links=links,
                children_id=[],
                parents_id=[],
                text=original_page,
                pagerank=-1.0,
                size=size,
                freq_words={}
            ))
            if parent_id is not None:
                self.pages[page_id].parents_id.append(parent_id)
                self.pages[parent_id].children_id.append(page_id)
                self.connectivity_matrix[parent_id, page_id] = 1
            for link in links:
                if link not in self.page_to_id:
                    self.url_queue.put((link, page_id))
                else:
                    next_page_id = self.page_to_id[link]
                    self.pages[page_id].children_id.append(next_page_id)
                    self.pages[next_page_id].parents_id.append(page_id)
            self.bar.set_description(f"{url}")
            self.bar.update()

    def worker(self):
        while self.url_queue.unfinished_tasks>0:
            try:
                url,parent_id = self.url_queue.get(timeout=1)
                self.crawl(url,parent_id)
                self.url_queue.task_done()
            except:
                continue

    def crawl_and_pagerank(self,num_workers=10) -> Tuple[List[Page], dict, np.ndarray]:
        # multithreading crawler
        self.bar=tqdm(total=self.max_pages)
        self.connectivity_matrix = np.zeros((self.max_pages, self.max_pages))
        self.url_queue.put((self.initial_url,None))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            for _ in range(num_workers):
                executor.submit(self.worker)
            self.url_queue.join()
        print("Finished!")
        # pagerank
        actual_pages=len(self.pages)
        self.connectivity_matrix=self.connectivity_matrix[:actual_pages,:actual_pages]
        pagerank = PageRank(0.8).compute(self.connectivity_matrix)
        for page, pr in zip(self.pages, pagerank):
            page.pagerank = pr
        if self.dump_dir is not None:
            self.dump_pages(self.pages, self.dump_dir)
        return self.pages, self.page_to_id, self.connectivity_matrix

    @staticmethod
    def dump_pages(pages: List[Page], dump_dir):
        if not os.path.exists(dump_dir):
            os.mkdir(dump_dir)
        page_text_dir = os.path.join(dump_dir, "original_pages/")
        page_metadata_path = os.path.join(dump_dir, "metadata.json")
        if not os.path.exists(page_text_dir):
            os.mkdir(page_text_dir)
        metadata = []
        for p in pages:
            metadata.append({
                "id": p.id,
                "title": p.title,
                "url": p.url,
                "last_modified": p.last_modified,
                "links": p.links,
                "children_id": p.children_id,
                "parents_id": p.parents_id,
                "pagerank": p.pagerank,
                "size":p.size,
                "freq_words":p.freq_words
            })
            page_text_path = os.path.join(page_text_dir, f"{p.id}.html")
            with open(page_text_path, "w", encoding="utf-8") as f:
                f.write(re.sub(r"[\x00-\x1F\x7F]", "", p.text))
        with open(page_metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
