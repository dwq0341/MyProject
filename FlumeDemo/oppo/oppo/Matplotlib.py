
import numpy as np
from matplotlib import font_manager
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False  # 解决保存图像时符号-显示为方块的2问题
y_1 = [1,0,1,1,2,4,3,2,3,4,4,5,6,5,4,3,3,1,1,1]
y_2 = [1,0,1,1,2,6,3,0,3,4,6,5,3,5,4,3,3,1,1,1]
x = range(11, 31)

plt.figure(figsize=(20, 8), dpi=60)
plt.plot(x, y_1, label='自己')
plt.plot(x, y_2, label='同桌')

_xtick_labels = ["{}岁".format(i) for i in x]
# plt.xticks(x, _xtick_labels, fontproperties=my_font)
# plt.yticks(range(0, 9))

plt.grid(alpha=0.1)

plt.legend(loc='upper left')

# plt.ion()
# plt.pause(15)
plt.show()