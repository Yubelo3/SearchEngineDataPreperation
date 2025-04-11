from dataclasses import dataclass
from typing import List


@dataclass
class Page:
    id: int  # page id, starts from 0
    title: str
    url: str
    last_modified: str  # last modified time in string
    links: List[str]  # in-page links
    children_id: List[int]  # page id for all pages pointed by this page
    parents_id: List[int]  # page id for all page that point to this page
    text: str  # original text content
    size: int
    pagerank: float
    freq_words: List[dict]  # 5 most frequent words

    @staticmethod
    def from_metadata(metadata: dict, html_filepath: str) -> "Page":
        with open(html_filepath, "r") as f:
            text = f.read()
        page = Page(
            id=metadata["id"],
            title=metadata["title"],
            url=metadata["url"],
            last_modified=metadata["last_modified"],
            links=metadata["links"],
            children_id=metadata["children_id"],
            parents_id=metadata["parents_id"],
            text=text,
            size=metadata["size"],
            pagerank=metadata["pagerank"],
            freq_words=metadata["freq_words"]
        )
        return page
