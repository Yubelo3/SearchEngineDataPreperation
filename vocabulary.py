class Vocabulary(object):
    def __init__(self) -> None:
        self.vocab = {}

    def map(self, word: str):
        if word not in self.vocab:
            self.vocab[word] = len(self.vocab)
        return self.vocab[word]

    def dictionary(self):
        return self.vocab
