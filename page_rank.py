import numpy as np


class PageRank(object):
    def __init__(self, damping_factor=0.8) -> None:
        self.d = damping_factor

    def compute(self, connectivity_matrix: np.ndarray, max_iter=1000):
        M = connectivity_matrix.astype(np.float32).T
        C = connectivity_matrix.sum(axis=-1,keepdims=True).T
        C[C == 0.0] = 1e-5
        M/=C
        pr_norm = float(M.shape[0])
        pr = np.ones((M.shape[0],))
        next_pr = np.zeros_like(pr)

        cur_iter = 0
        while np.abs(pr-next_pr).sum() > 1e-5 and cur_iter < max_iter:
            pr = next_pr
            next_pr = (1-self.d)+self.d*M@pr.T
            pr_sum = next_pr.sum()
            next_pr *= pr_norm/pr_sum
            cur_iter += 1
        return pr

if __name__=="__main__":
    M=np.array([
        [1,0,1],
        [0,1,0],
        [1,1,0],
    ])
    pr=PageRank(0.8).compute(M)
    print(pr)