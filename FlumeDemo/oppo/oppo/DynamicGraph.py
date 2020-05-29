import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False  # 解决保存图像时符号-显示为方块的2问题
# plt.axis([0, 100, 0, 1])
# plt.ion()

# xs = [0, 0]
# ys = [1, 1]
# for i in range(100):
#     y = np.random.random()
#     xs[0] = xs[1]
#     ys[0] = ys[1]
#
#     xs[1] = iplt.legend()
#     ys[1] = y
#     plt.plot(xs, ys)
#     plt.pause(0.1)

x_data = ['2011', '2012', '2013', '2014', '2015', '2016', '2017']
y_data = [58000, 60200, 63000, 71000, 84000, 90500, 107000]
y_data2 = [52000, 54200, 64500, 82300, 85800, 91500, 107000]
y_data3 = [57000, 54300, 65500, 81300, 86800, 92500, 117100]
y_data4 = [59000, 54400, 66500, 83300, 87800, 93500, 122720]
y_data5 = [60000, 54500, 67500, 84300, 88800, 94500, 132700]

ln1, = plt.plot(x_data, y_data, color='red', linewidth=2.0, linestyle='--')
ln2, = plt.plot(x_data, y_data2, color='blue', linewidth=3.0, linestyle='-.')
ln3, = plt.plot(x_data, y_data3, color='red', linewidth=3.0, linestyle='-.')
ln4, = plt.plot(x_data, y_data4, color='blue', linewidth=3.0, linestyle='-.')
ln5, = plt.plot(x_data, y_data5, color='red', linewidth=3.0, linestyle='-.')

# my_font = fm.FontProperties(fname="/usr/share/fonts/wqy-microhei/wqy-microhei.ttc")

# plt.title("电子产品销售量", fontproperties=my_font)  # 设置标题及字体

plt.legend(handles=[ln1, ln2, ln3, ln4, ln5], labels=['鼠标的年销量', '键盘的年销量'])

ax = plt.gca()
ax.spines['right'].set_color('none')  # right边框属性设置为none 不显示
ax.spines['top'].set_color('none')  # top边框属性设置为none 不显示
plt.savefig('demo.png', bbox_inches='tight')
plt.show()