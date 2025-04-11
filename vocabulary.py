class Vocabulary(object):
    def __init__(self) -> None:
        self.vocab = {}
        self.invert_vocab = []

    def map(self, word: str):
        if word not in self.vocab:
            self.vocab[word] = len(self.vocab)
            self.invert_vocab.append(word)
        return self.vocab[word]

    def dictionary(self):
        return self.vocab

    def invert_dictionary(self):
        return self.invert_vocab
