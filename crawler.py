from page_parser import PageParser
from collections import deque
from page import Page
from typing import List, Tuple
import numpy as np
from tqdm import tqdm
import json
import os
import re
from page_rank import PageRank


class Crawler(object):
    def __init__(self) -> None:
        self.parser = PageParser()

    def crawl_and_pagerank(self, initial_url: str, num_pages=300, dump_dir="page_data") -> Tuple[List[Page], dict, np.ndarray]:
        # can use sparse matrix for connectivity_matrix if num_pages is large
        connectivity_matrix = np.zeros((num_pages, num_pages))
        pages: List[Page] = []
        page_to_id = {}
        queue = deque()
        queue.append((initial_url, None))  # page_to_visit, parent_id
        num_crawled = 0
        progress_bar = tqdm(total=num_pages, desc="retriving...")
        while num_crawled < num_pages and len(queue) > 0:
            url, parent_id = queue.popleft()
            progress_bar.set_description(f"retriving {url}...")
            page = self.parser.extract_webpage(url)
            title, last_modified, links, original_page,size = page["title"], page[
                "last_modified"], page["links"], page["original_page"],page["size"]

            page_key = (url, last_modified)
            page_id = num_crawled
            page_to_id[page_key] = num_crawled
            
            pages.append(Page(
                id=num_crawled,
                title=title,
                url=url,
                last_modified=last_modified,
                links=links,
                children_id=[],
                parents_id=[],
                text=original_page,
                pagerank=-1.0,
                size=-1,
                freq_words={}
            ))
            num_crawled += 1
            progress_bar.update()
            if parent_id is not None:
                pages[page_id].parents_id.append(parent_id)
                pages[parent_id].children_id.append(page_id)
                connectivity_matrix[parent_id, page_id] = 1
            for link in links:
                next_page_modified_time = self.parser.extract_webpage(link)[
                    "last_modified"]
                next_page_key = (link, next_page_modified_time)
                if next_page_key not in page_to_id:
                    queue.append((link, page_id))
                else:
                    next_page_id = page_to_id[next_page_key]
                    pages[page_id].children_id.append(next_page_id)
                    pages[next_page_id].parents_id.append(page_id)

        # add pagerank value to page information
        actual_pages=len(pages)
        connectivity_matrix=connectivity_matrix[:actual_pages,:actual_pages]
        pagerank = PageRank(0.8).compute(connectivity_matrix)
        for page, pr in zip(pages, pagerank):
            page.pagerank = pr
        if dump_dir is not None:
            self.dump_pages(pages, dump_dir)
        return pages, page_to_id, connectivity_matrix

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
