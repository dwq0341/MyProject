import math
import pandas as np
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
# fig2, ax2 = plt.subplots()

{
  "DEST_IP": "8.8.8.8",
  "DESTIP_CNT": 2093066
}
{
  "DEST_IP": "59.37.130.0",
  "DESTIP_CNT": 265
}
{
  "DEST_IP": "114.114.114.114",
  "DESTIP_CNT": 875639
}
{
  "DEST_IP": "114.114.114.114",
  "DESTIP_CNT": 875649
}
y1 = []
y2 = []
y3 = []
y4 = []
y5 = []
y6 = []
y7 = []
y8 = []
y9 = []
y10 = []
for i in range(50):
    y1.append(math.sin(i))
    y2.append(math.cos(i))
    ax.cla()
    ax.set_title("Loss")
    ax.set_xlabel("Iteration")
    ax.set_ylabel("Loss")
    ax.set_xlim(0, 55)
    ax.set_ylim(-1, 1)
    ax.grid()
    ax.plot(y1, label='train')
    ax.plot(y2, label='test')
    ax.legend(loc='best')

    # ax2.cla()
    # ax2.set_title("Loss")
    # ax2.set_xlabel("Iteration")
    # ax2.set_ylabel("Loss")
    # ax2.set_xlim(0, 55)
    # ax2.set_ylim(-1, 1)
    # ax2.grid()
    # ax2.plot(y1, label='train')
    # ax2.plot(y2, label='test')
    # ax2.legend(loc='best')
    plt.pause(2)