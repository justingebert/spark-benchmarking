#!/usr/bin/env python3
"""
Automated benchmark runner for TTR Spark Job.
Runs spark-submit with different core counts on multiple datasets.
"""
import subprocess
import re
import json
import os
import time

CORES_LIST = [1, 2, 4, 8]
OUTPUT_FILE = "results/benchmark_data_multi.json"
JAR_PATH = "target/prog-alg-1.0.jar"
STOPWORDS_DIR = "data/stopwords"

DATASETS = {
    "134MB": "data/text",
    "672MB": "data/text_large",
    "2.6GB": "data/text_huge"
}

def build_project():
    print("Building project with Maven...")
    subprocess.run(["mvn", "clean", "package", "-q"], check=True)
    print("Build complete.")

def run_spark_benchmark(cores, data_dir):
    print(f"    Running Spark with {cores} cores...")
    cmd = [
        "spark-submit",
        "--class", "TTRJob",
        "--master", f"local[{cores}]",
        "--driver-memory", "10g", 
        "--executor-memory", "10g",
        JAR_PATH,
        data_dir,
        STOPWORDS_DIR,
        "results_temp"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        # Extract execution time
        match = re.search(r"Execution time: (\d+\.\d+) seconds", result.stdout)
        if match:
            return float(match.group(1))
    except subprocess.CalledProcessError as e:
        print(f"    Error: {e}")
        # print(e.stderr) # Optional: verify memory errors
    return None

def run_serial_benchmark(data_dir):
    print("    Running Serial Benchmark...")
    cmd = [
        "java", "-Xmx10g", "-cp", JAR_PATH, "TTRSerialJob",
        data_dir,
        STOPWORDS_DIR
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        match = re.search(r"Execution time: (\d+\.\d+) seconds", result.stdout)
        if match:
            return float(match.group(1))
    except subprocess.CalledProcessError as e:
        print(f"    Error: {e}")
    return None

def main():
    build_project()
    
    overall_results = {}

    for name, path in DATASETS.items():
        print(f"\n=== Benchmarking Dataset: {name} ({path}) ===")
        if not os.path.exists(path):
            print(f"Skipping {name}, path not found.")
            continue

        dataset_results = {
            "cores": [],
            "spark_times": [],
            "speedups_vs_spark1": [],
            "serial_time": None
        }

        # 1. Serial Run
        serial_time = run_serial_benchmark(path)
        dataset_results["serial_time"] = serial_time
        if serial_time:
            print(f"    -> Serial Time: {serial_time}s")

        # 2. Spark Runs
        spark_1_time = None
        for cores in CORES_LIST:
            t = run_spark_benchmark(cores, path)
            if t:
                dataset_results["cores"].append(cores)
                dataset_results["spark_times"].append(t)
                
                # Capture baseline for speedup (Spark 1 core)
                if cores == 1:
                    spark_1_time = t
                
                speedup = 0.0
                if spark_1_time:
                    speedup = spark_1_time / t
                
                dataset_results["speedups_vs_spark1"].append(round(speedup, 2))
                print(f"    -> Spark {cores} Cores: {t}s (Speedup: {speedup:.2f}x)")

        overall_results[name] = dataset_results

    # Save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(overall_results, f, indent=2)
    print(f"\nSaved results to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
