from snowballstemmer import EnglishStemmer
from vocabulary import Vocabulary
import re
import wordninja
import math
import unicodedata
import string


class Stemmer:
    def __init__(self, stopword_file: str, whitelist=["crawler"]) -> None:
        self.stemmer = EnglishStemmer()
        self.vocab = Vocabulary()
        self.stopwords = set()
        self.punctionation_token=" 990990990 "
        with open(stopword_file, "r") as f:
            for word in f.readlines():
                self.stopwords.add(word.strip())
        assert "is" in self.stopwords
        for w in whitelist:
            wordlist_len = len(wordninja.DEFAULT_LANGUAGE_MODEL._wordcost)
            wordninja.DEFAULT_LANGUAGE_MODEL._wordcost[w] = math.log(
                wordlist_len * math.log(wordlist_len)
            )

    def replace_punctuation_and_non_alpha(self,text):
        text=text.replace("-",self.punctionation_token)
        # punctuations = r'[!"#$%&\'()*+,./:;<=>?@\[\\\]^_`{|}~]'
        step1 = re.sub(f"[{re.escape(string.punctuation)}]", self.punctionation_token, text)
        # print(step1)
        step2 = re.sub(r'[^a-zA-Z0-9\s]', ' ', step1)
        result = re.sub(r'\s+', ' ', step2).strip()
        return result
    
    def clean_text(self, text: str) -> str:
        text = self.remove_accents(text).lower()  # remove accent
        lookaround_pattern = r'(?<=[a-zA-Z])-(?=[a-zA-Z])'
        text = re.sub(lookaround_pattern, ' ', text)
        text=text.replace("-",self.punctionation_token)
        # remove unrecognized character
        cleaned_text=self.replace_punctuation_and_non_alpha(text)
        # print(cleaned_text)
        cleaned_text = [w for w in cleaned_text.split() if w.isalnum()]
        splited_text = []
        for w in cleaned_text:
            splited_text += wordninja.split(w)  # handle bad concatenation

        # print(splited_text)
        stopword_removed_text, stopword_removed_index = [], []
        for i, w in enumerate(splited_text):
            if w not in self.stopwords:
                stopword_removed_text.append(w)
                stopword_removed_index.append(i)
        stopword_removed_text = " ".join(stopword_removed_text)
        return stopword_removed_text, stopword_removed_index

    def remove_accents(self, text):
        normalized = unicodedata.normalize("NFKD", text)
        return "".join(
            c for c in normalized if not unicodedata.combining(c) and ord(c) < 128
        )

    def stem(self, word: str):
        return self.stemmer.stemWord(word)

    def stem_and_map(self, content: str):
        text, index = self.clean_text(content)
        output = [self.vocab.map(self.stem(w)) for w in text.split(" ")]
        return output, index

    def vocabulary(self):
        return self.vocab




if __name__ == "__main__":
    stemmer = Stemmer("stopwords.txt")
    # print(stemmer.stem("changing"))
    # print(stemmer.stem("quickly"))
    # print(stemmer.stem("news"))
    print(stemmer.stem("see"))
    # print(wordninja.split("ratingsrecommendationsmessage"))

    # s=stemmer.clean_text("human-readable this is a test, I want to replace, punctuation with special token! test on human-readable strings... try to make it feasible? ")
    # print(s)
    # s1=stemmer.clean_text("human-readable")  # should be [0,1]
    # print(s1)
    # s2=stemmer.clean_text("human- readable")  # should be [0,2]
    # print(s2)
    # s3=stemmer.clean_text("human - readable")  # should be [0,2]
    # print(s3)
    # s4=stemmer.clean_text("human 123-readable")
    # print(s4)
    # s5=stemmer.clean_text("is- 123-456 is-")  # is占一位，非连词-占一位
    # print(s5)



