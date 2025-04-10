import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import re


class PageParser(object):
    def __init__(self) -> None:
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def looks_like_webpage(self, url: str):
        '''
        returns if a hyperlink looks like webpage (instead of other common resource like .png)
        seems not necessary, the given test link is pretty friendly
        every link you can find within the page is an .htm
        '''
        # last_part = url.split('/')[-1].lower()
        # return (not last_part or
        #     '.' not in last_part or
        #     last_part.endswith(('.html', '.htm', '.php', '.asp')))
        return True

    def extract_webpage(self, url: str):
        '''
        returns {"title":str,"last_modified":str,"links":List[str],"original_page":str}
        '''
        # try:
        # extract title and body as string
        response = requests.get(url, headers=self.headers, timeout=10)
        response.encoding = "utf-8"
        response.raise_for_status()

        last_modified = response.headers.get('Last-Modified')
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string if soup.title else None

        # extract in-page links
        links = set()
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            absolute_url = urljoin(url, href)
            if not self.looks_like_webpage(absolute_url):
                continue
            links.add(absolute_url)
        # except:
        #     print(f"WARNING: failed to retrieve {url}")
        #     return None

        return {
            "title": title,
            "last_modified": last_modified,
            "links": list(links),
            "original_page": response.text
        }

    def extract_title_and_body_from_html_str(self, content: str):
        soup = BeautifulSoup(content, "html.parser",from_encoding="utf-8")
        title_text = soup.title.string
        for element in soup.body(["script", "style", "nav", "footer"]):
            element.decompose()
        body_text = soup.body.get_text(separator=" ", strip=True)
        return title_text, body_text


if __name__ == "__main__":
    parser = PageParser()
    with open("page_data/original_pages/15.html", "r") as f:
        content = f.read()
    title, body = parser.extract_title_and_body_from_html_str(content)
    print(body)
