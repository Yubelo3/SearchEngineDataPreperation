# Data Preperation

* word到word_id的映射放在page_data/dictionary.json  
* page_url到page_id以及其他页面相关信息的映射放在page_data/metadata.json  
* page_id到这个页面包含的word对应的word_id的映射放在page_data/forward_index.json  
* word_id到包含这个单词的page对应的page_id的映射放在page_data/title_inverted_index.json和page_data/body_inverted_index.json

## What does this project do  
* Crawl pages starts from `"https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"`  
* Compute pagerank from link info.  
* Store page metadata in `page_data/metadata.json`  
* Perform stopword removal and stemming, remove accents, fix word concatenation errors.  
* Build vocabulary book (map from word to word_id), in `page_data/dictionary.json`.  
* Build forward index (map from page to word) in `page_data/forward_index.json`.  
* Build inverted index (map from word to page) for title and body respectively in `page_data/title_inverted_index.json` and `page_data/body_inverted_index.json`.   

## Project Structure

`main.py`: the main script.  
`crawler.py`: a crawler to perform web crawling in a BFS manner.  
`page_parser.py`: extract page informations from a given url.  
`page.py`: defination for dataclass `Page`
`stemmer.py`: a stemmer which performs cleaning, splitting, and stemming
`vocabulary.py`: a vocabulary book that maps word to word_index
`page_rank.py`: a class used to compute pagerank given a connectivity matrix

## Output Format Specification  

### `page_data/dictionary.json` 
A dict[str -> int], map ***stemmed_word*** to ***word_id***  

### `page_data/metadata.json`  
A `list` of `dict`, each `dict` stores the basic information of one page.  
This `list` is sorted by page_id in ascending order.   
* "id": int, page_id.  
* "url": str, page's absolute url.  
* "title": str, page's title (in original form, not stemmed).  
* "last_modified": str, last modified time.  
* "links": List[str], all links in this page (in absolute url form).  
* "children_id": List[int], page_id for those pages pointed by this page.  
* "parents_id": List[int], page_id for those pages point to this page.  
* "pagerank": float, pagerank value for this page. Note that pagerank are normalized so that the summation of pr value is equal to number of pages.  
* "size": int, html file size  
* "freq_words": a dict that map top-5 frequent words to its frequency

### `page_data/forward_index.json`  
This is the index that map from page to in-page words.  
A `list` of `dict`, each `dict` stores the word_id of its title and body ***after performming stemming & stopword removal***.  
This `list` is sorted by page_id in ascending order.   
* "id": int, page_id.  
* "title": List[int], word_id for words in the title of this page.  
* "body": List[int], word_id for words in the body of this page.  

### `page_data/title_inverted_index.json`  
This is the index that map from word to pages that contains this word.  
It is a `list` of `dict`, each `dict` represents a word.  
* "id": int, word_id  
* "doc": A `list` of 2-elem `list`, each 2-elem `list` is [doc_id, number_of_occurence_in_this_doc]  

### `page_data/body_inverted_index.json`  
Same as `title_inverted_index.json`.  
