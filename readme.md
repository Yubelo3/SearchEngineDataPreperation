# Data Preperation

## Requirements
The crawled data has already store in `page_data`. If you'd like to run it by yourself,  
```bash
conda env create -f requirements.yml
conda activate crawler
python main.py
```
If you don't want to create conda environment,  
```bash
pip install -r requirements.txt
python main.py
```

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
* "title_word_pos": List[int], word position for every title word before stopword removal.  
* "body": List[int], word_id for words in the body of this page.  
* "body_word_pos: List[int], word_id for every body word before stopword removal.  

### `page_data/title_inverted_index.json`  
This is the index that map from word to pages that contains this word.  
It is a `list` of `dict`, each `dict` represents a word.  
* "id": int, word_id  
* "doc": A `list` of 3-elem `list`, each 3-elem `list` is [doc_id, number_of_occurence_in_this_doc, [positions of all occurrences of the character in the string]]  

### `page_data/body_inverted_index.json`  
Same as `title_inverted_index.json`.  
