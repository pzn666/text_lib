import numpy as np
import matplotlib.pyplot as plt
import matplotlib.lines as mlines


class LossVisualizer:
    def __init__(self):
        self.fig = plt.gcf()

    def visualize(self, data):
        plt.plot(*zip(*data))
        plt.show()

    def save(self, path):
        self.fig.savefig(path)
