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
    pagerank: float
