from crawler import Crawler
from stemmer import Stemmer
from page_parser import PageParser
import os
import json
from tqdm import tqdm
from typing import List
from page import Page


INITIAL_URL = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
PAGE_DIR = "page_data"


def main():
    # pages, page_to_id, connectivity_matrix = crawl_pages()
    forward_index, vocabulary = stemming()
    title_inverted_index, body_inverted_index = build_inverted_index()


def crawl_pages():
    '''
    crawl pages and save to `$PAGE_DIR/original_pages/$doc_id.html`
    also save the metadata `$PAGE_DIR/metadata.json`
    '''
    crawler = Crawler()
    return crawler.crawl_and_pagerank(INITIAL_URL, dump_dir=PAGE_DIR)


def stemming():
    '''
    perform stopword removal & stemming on page title and body
    save stemmed results (forward index) to `$PAGE_DIR/forward_index.json`
    save dictionary (word->word_id) to `$PAGE_DIR/dictionary.json`
    '''
    parser = PageParser()
    stemmer = Stemmer("stopwords.txt")
    html_dir = os.path.join(PAGE_DIR, "original_pages/")
    metadata_path=os.path.join(PAGE_DIR,"metadata.json")
    with open(metadata_path,"r") as f:
        metadata=json.load(f)
    pages=[None for _ in range(len(metadata))]
    forward_index = []
    # stemming, build forward index
    for file in tqdm(os.listdir(html_dir), desc="stemming..."):
        filepath = os.path.join(html_dir, file)
        doc_id = int(file.split(".")[0])
        pages[doc_id]=Page.from_metadata(metadata[doc_id],filepath)
        pages[doc_id].size=os.path.getsize(filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            html_content = f.read()
        title, body = parser.extract_title_and_body_from_html_str(html_content)
        stemmed_title = stemmer.stem_and_map(title)
        stemmed_body = stemmer.stem_and_map(body)
        all_words=stemmed_title+stemmed_body
        freq_counter={}
        for w in all_words:
            stemmed_word=stemmer.vocabulary().invert_dictionary()[w]
            freq_counter[stemmed_word]=1 if stemmed_word not in freq_counter else freq_counter[stemmed_word]+1
        freq_words = dict(sorted(freq_counter.items(), key=lambda item: item[1], reverse=True)[:min(5,len(freq_counter))])
        pages[doc_id].freq_words=freq_words
        forward_index.append({
            "id": doc_id,
            "title": stemmed_title,
            "body": stemmed_body,
        })
    Crawler.dump_pages(pages,PAGE_DIR)
    forward_index.sort(key=lambda x: x["id"])
    forward_index_path = os.path.join(PAGE_DIR, "forward_index.json")
    with open(forward_index_path, "w", encoding="utf-8") as f:
        json.dump(forward_index, f)
    with open(os.path.join(PAGE_DIR, "dictionary.json"), "w") as f:
        json.dump(stemmer.vocabulary().dictionary(), f)
    return forward_index, stemmer.vocabulary()


def build_inverted_index():
    forward_index_path = os.path.join(PAGE_DIR, "forward_index.json")
    dictionary_path = os.path.join(PAGE_DIR, "dictionary.json")
    with open(dictionary_path, "r") as f:
        dictionary = json.load(f)
        vocab_size = len(dictionary)
    with open(forward_index_path, "r") as f:
        forward_index = json.load(f)
    title_inverted_index = [{"id": i, "doc": []} for i in range(vocab_size)]
    body_inverted_index = [{"id": i, "doc": []} for i in range(vocab_size)]

    def aggregate(page_id, word_list, target_index):
        for w in word_list:
            if len(target_index[w]["doc"]) > 0 and target_index[w]["doc"][-1][0] == page_id:
                target_index[w]["doc"][-1][1] += 1
            else:
                target_index[w]["doc"].append([page_id, 1])
    for page in forward_index:
        page_id = page["id"]
        aggregate(page_id, page["title"], title_inverted_index)
        aggregate(page_id, page["body"], body_inverted_index)
    title_inverted_index_path = os.path.join(
        PAGE_DIR, "title_inverted_index.json")
    body_inverted_index_path = os.path.join(
        PAGE_DIR, "body_inverted_index.json")
    with open(title_inverted_index_path, "w") as f:
        json.dump(title_inverted_index, f)
    with open(body_inverted_index_path, "w") as f:
        json.dump(body_inverted_index, f)

    return title_inverted_index, body_inverted_index


if __name__ == "__main__":
    main()
