from nltk.stem import PorterStemmer
from vocabulary import Vocabulary
import re
import wordninja
import math
import unicodedata


class Stemmer():
    def __init__(self, stopword_file: str, whitelist=["crawler"]) -> None:
        self.stemmer = PorterStemmer()
        self.vocab = Vocabulary()
        self.stopwords = set()
        with open(stopword_file, "r") as f:
            for word in f.readlines():
                self.stopwords.add(word.strip())
        for w in whitelist:
            wordlist_len = len(wordninja.DEFAULT_LANGUAGE_MODEL._wordcost)
            wordninja.DEFAULT_LANGUAGE_MODEL._wordcost[w] = math.log(
                wordlist_len*math.log(wordlist_len))

    def clean_text(self, text: str) -> str:
        text = self.remove_accents(text)  # remove accent
        # remove unrecognized character
        cleaned_text = re.sub(r'[^\w\s]', ' ', text)
        cleaned_text = [w.lower() for w in cleaned_text.split() if w.isalnum()]
        splited_text = []
        for w in cleaned_text:
            splited_text += wordninja.split(w)  # handle bad concatenation

        stopword_removed_text, stopword_removed_index = [], []
        for i, w in enumerate(splited_text):
            if w not in self.stopwords:
                stopword_removed_text.append(w)
                stopword_removed_index.append(i)
        splited_text = ' '.join(splited_text)
        return splited_text, stopword_removed_index

    def remove_accents(self, text):
        normalized = unicodedata.normalize('NFKD', text)
        return ''.join(c for c in normalized if not unicodedata.combining(c) and ord(c) < 128)

    def stem(self, word: str):
        return self.stemmer.stem(word)

    def stem_and_map(self, content: str):
        text, index = self.clean_text(content)
        output = [self.vocab.map(self.stem(w)) for w in text.split(" ")]
        return output, index

    def vocabulary(self):
        return self.vocab


if __name__ == "__main__":
    stemmer = Stemmer()
    print(stemmer.stem("changing"))
    print(stemmer.stem("quickly"))
    print(stemmer.stem("movement"))
