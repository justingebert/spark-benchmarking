import matplotlib.pyplot as plt
import json
import os
import numpy as np

JSON_FILE = "results/benchmark_data_multi.json"

def main():
    if not os.path.exists(JSON_FILE):
        print(f"Error: {JSON_FILE} not found. Run benchmarks first.")
        # Create dummy data for testing if needed, or just exit
        return

    with open(JSON_FILE, 'r') as f:
        data = json.load(f)

    # Style
    plt.style.use('seaborn-v0_8-whitegrid')
    
    datasets = ["134MB", "672MB", "2.6GB"]
    
    # ==========================================
    # 1. Comparison Charts (Time)
    # ==========================================
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    
    for idx, name in enumerate(datasets):
        ax = axes[idx]
        if name not in data:
            ax.text(0.5, 0.5, "No Data", ha='center')
            continue
            
        d = data[name]
        
        # Prepare data for plotting
        x_labels = ["Serial"] + [f"Spark {c}C" for c in d["cores"]]
        times = [d["serial_time"] if d["serial_time"] else 0] + d["spark_times"]
        colors = ['#C44E52'] + ['#4C72B0'] * len(d["cores"]) # Red for Serial, Blue for Spark
        
        # Plot
        bars = ax.bar(x_labels, times, color=colors, edgecolor='black')
        
        ax.set_title(f"Dataset: {name}", fontsize=14, fontweight='bold')
        ax.set_ylabel("Execution Time (s)")
        ax.set_xticklabels(x_labels, rotation=45, ha='right')
        
        # Labels
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.1f}s',
                        ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.suptitle("Total Execution Time: Serial vs Spark", fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig("latex/images/benchmark_comparison.png")
    print("Saved comparison chart.")

    # ==========================================
    # 2. Speedup Chart (Scalability)
    # ==========================================
    plt.figure(figsize=(10, 6))
    
    markers = ['o', 's', '^']
    colors = ['#55A868', '#CCB974', '#4C72B0'] # Green, Yellow, Blue
    
    for i, name in enumerate(datasets):
        if name not in data: continue
        d = data[name]
        if not d["speedups_vs_spark1"]: continue
        
        plt.plot(d["cores"], d["speedups_vs_spark1"], 
                 marker=markers[i], color=colors[i], linewidth=2.5, markersize=8,
                 label=f"{name}")

    # Ideal Linear Line
    max_cores = 8
    plt.plot([1, max_cores], [1, max_cores], '--', color='gray', alpha=0.5, label='Ideal Linear')
    
    plt.title("Spark Scalability (Speedup relative to Spark 1-Core)", fontsize=14, fontweight='bold')
    plt.xlabel("Number of Spark Cores")
    plt.ylabel("Speedup (x-times)")
    plt.legend()
    plt.xticks([1, 2, 4, 8])
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    plt.savefig("latex/images/speedup_comparison.png")
    print("Saved speedup chart.")

if __name__ == "__main__":
    main()