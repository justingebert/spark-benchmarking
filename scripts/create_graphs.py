import matplotlib.pyplot as plt
import numpy as np

# Benchmark data from our tests
cores = [1, 2, 4, 8]
times = [54.83, 28.84, 18.02, 13.52]
speedups = [1.0, 54.83/28.84, 54.83/18.02, 54.83/13.52]

# Set up style
plt.style.use('seaborn-v0_8-whitegrid')
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# ===== Graph 1: Execution Time =====
ax1.bar(cores, times, color='#4C72B0', edgecolor='black', linewidth=1.2)
ax1.set_xlabel('Anzahl Kerne', fontsize=12)
ax1.set_ylabel('Laufzeit (Sekunden)', fontsize=12)
ax1.set_title('Laufzeit vs. Anzahl Kerne\n(672 MB Datensatz)', fontsize=14, fontweight='bold')
ax1.set_xticks(cores)

# Add value labels on bars
for i, (c, t) in enumerate(zip(cores, times)):
    ax1.text(c, t + 1, f'{t:.1f}s', ha='center', va='bottom', fontsize=11, fontweight='bold')

# ===== Graph 2: Speedup =====
ax2.plot(cores, speedups, 'o-', color='#55A868', linewidth=2.5, markersize=10, label='Gemessener Speedup')
ax2.plot(cores, cores, '--', color='#C44E52', linewidth=2, alpha=0.7, label='Idealer Speedup (linear)')
ax2.set_xlabel('Anzahl Kerne', fontsize=12)
ax2.set_ylabel('Speedup (x-fach)', fontsize=12)
ax2.set_title('Skalierungsverhalten\n(Speedup vs. Kerne)', fontsize=14, fontweight='bold')
ax2.set_xticks(cores)
ax2.set_yticks(range(1, 9))
ax2.legend(loc='upper left', fontsize=10)
ax2.set_xlim(0.5, 8.5)
ax2.set_ylim(0.5, 8.5)

# Add speedup labels
for c, s in zip(cores, speedups):
    ax2.annotate(f'{s:.1f}x', (c, s), textcoords="offset points", xytext=(0, 10), 
                 ha='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('latex/images/benchmark.png', dpi=150, bbox_inches='tight')
plt.savefig('latex/images/benchmark.pdf', bbox_inches='tight')
print("Saved: latex/images/benchmark.png and benchmark.pdf")

# ===== Graph 3: TTR Comparison by Language =====
fig2, ax3 = plt.subplots(figsize=(10, 6))

languages = ['russian', 'dutch', 'ukrainian', 'spanish', 'german', 'italian', 'english', 'french']
ttr_values = [0.2940, 0.2012, 0.2004, 0.1070, 0.0723, 0.0679, 0.0349, 0.0338]

colors = plt.cm.RdYlGn(np.linspace(0.8, 0.2, len(languages)))
bars = ax3.barh(languages, ttr_values, color=colors, edgecolor='black', linewidth=1.2)

ax3.set_xlabel('Type-Token-Ratio (TTR)', fontsize=12)
ax3.set_title('Sprachliche Vielfalt pro Sprache\n(höher = mehr Vielfalt)', fontsize=14, fontweight='bold')
ax3.set_xlim(0, 0.35)

# Add value labels
for bar, val in zip(bars, ttr_values):
    ax3.text(val + 0.005, bar.get_y() + bar.get_height()/2, f'{val:.3f}', 
             va='center', fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig('latex/images/ttr_comparison.png', dpi=150, bbox_inches='tight')
plt.savefig('latex/images/ttr_comparison.pdf', bbox_inches='tight')
print("Saved: latex/images/ttr_comparison.png and ttr_comparison.pdf")