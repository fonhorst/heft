import random
#import matplotlib.pyplot as plt
#import numpy as np

for _ in range(1000):
    res = []
    for _ in range(4):
        res.append(random.randint(10,30))
    rel = 80 / sum(res)
    res = [round(val * rel) for val in res]
    res = [30 if val > 30 else val for val in res]
    div = 80 - sum(res)
    if div > 0:
        res[res.index(min(res))] += div
    if div < 0:
        res[res.index(max(res))] += div
    if (sum(res) != 80) | (max(res) > 30):
        print("DANGER")


"""
#HIST
mu, sigma = 0, 1
x = mu + sigma * np.random.randn(10000)
hist, bins = np.histogram(x, bins=50)
width = 0.7 * (bins[1] - bins[0])
center = (bins[:-1] + bins[1:]) / 2
plt.bar(center, hist, align='center', width=width)
plt.show()
"""

