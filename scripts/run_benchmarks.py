#!/usr/bin/env python3
"""
Automated benchmark runner for TTR Spark Job.
Runs spark-submit with different core counts and saves results to JSON.
"""
import subprocess
import re
import json
import os

CORES_LIST = [1, 2, 4, 8]
OUTPUT_FILE = "results/benchmark_data.json"
JAR_PATH = "target/prog-alg-1.0.jar"
DATA_DIR = "data/text_large"  # Using the large dataset for meaningful benchmarks
STOPWORDS_DIR = "data/stopwords"

def run_benchmark(cores):
    print(f"Running with {cores} cores...")
    cmd = [
        "spark-submit",
        "--class", "TTRJob",
        "--master", f"local[{cores}]",
        JAR_PATH,
        DATA_DIR,
        STOPWORDS_DIR,
        "results_temp"
    ]
    
    try:
        # Run command and capture output
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout
        
        # Extract execution time using regex
        match = re.search(r"Execution time: (\d+\.\d+) seconds", output)
        if match:
            return float(match.group(1))
        else:
            print(f"Error: Could not find execution time in output for {cores} cores.")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error running benchmark for {cores} cores: {e}")
        print(e.stderr)
        return None

def build_project():
    print("Building project with Maven...")
    subprocess.run(["mvn", "clean", "package", "-q"], check=True)
    print("Build complete.")

def main():
    if not os.path.exists(DATA_DIR):
        print(f"Error: Dataset {DATA_DIR} not found. Please create it first.")
        return

    build_project()

    results = {
        "cores": [],
        "times": [],
        "speedups": []
    }

    base_time = None

    for cores in CORES_LIST:
        time_taken = run_benchmark(cores)
        if time_taken is not None:
            results["cores"].append(cores)
            results["times"].append(time_taken)
            
            if base_time is None:
                base_time = time_taken
            
            speedup = base_time / time_taken
            results["speedups"].append(round(speedup, 2))
            
            print(f"  -> Time: {time_taken}s, Speedup: {speedup:.2f}x")
        else:
            print("Skipping due to error.")

    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nBenchmark complete. Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
