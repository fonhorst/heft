## all was calculated by 10.253.0.114
import numpy
import matplotlib.pyplot as plt

ga_m25 = [55.61, 57.93, 58.73, 56.39, 58.70, 56.73, 58.02, 55.97, 54.56, 58.13]
ga_m50 = [115.79, 114.02, 111.23, 113.63, 101.21, 105.35, 108.64, 110.34, 117.25, 112.45]
ga_m75 = [174.82, 190.44, 180.96, 192.18, 164.58, 178.67, 183.45, 186.89, 168.23, 170.89]
ga_m100 = [261.32, 262.57, 269.68, 273.31, 261.82, 265.47, 270.34, 268.56, 271.78, 262.33]

cga_m25 = [53.69, 54.36, 54.03, 53.91, 59.51, 53.78, 56.76, 55.46, 57.86, 52.65]
cga_m50 = [83.91, 85.55, 79.85, 81.46, 81.75, 84.43, 87.54, 82.67, 81.76, 85.12]
cga_m75 = [110.38, 107.83, 109.69, 110.23, 106.37, 105.32, 108.67, 112.34, 105.78,107.02]
cga_m100 = [131.53, 131.89, 132.35, 134.133, 131.839, 134.26, 133.35, 132.74, 131.98, 133.25]

cga_ga = [(cga_m25, ga_m25), (cga_m50, ga_m50), (cga_m75, ga_m75), (cga_m100, ga_m100)]

cga_ga_relation = [(1 - numpy.mean(vcga) / numpy.mean(vga))*100 for vcga, vga in cga_ga]


points = ['m25', 'm50', 'm75', 'm100']
plt.figure(figsize=(10, 10))
plt.grid(True)
ax = plt.gca()
ax.set_xlim(0, len(points))
ax.set_xscale('linear')
plt.xticks(range(0, len(points)))
ax.set_xticklabels(points)
ax.set_title("Speed up by cga")
ax.set_ylabel("improvement, %")
plt.setp(plt.xticks()[1], rotation=30, ha='right')


plt.plot([i for i in range(len(points))], cga_ga_relation, '-gD')

plt.show()
