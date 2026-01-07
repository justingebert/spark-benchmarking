# Spark Type-Token-Ratio

This project calculates the **Type-Token-Ratio (TTR)** for a multilingual text corpus using Apache Spark.
It measures linguistic diversity by comparing the number of unique words (Types) to the total number of words (Tokens).

## Implementation Details

- **Spark Job**: `TTRJob.java`
- **Features**:
  - `wholeTextFiles`: Reads files while preserving directory paths for language detection
  - **Broadcast Variables**: Efficiently distributes stopword lists to all workers
  - **Unicode Tokenization**: Supports Cyrillic (Russian/Ukrainian) and other scripts
  - **Type-Token Ratio**: Calculates `unique / total` for each language

## Repo Structure

- **data/stopwords/**: JSON stopword lists for 8 languages
- **data/text/<language>/**: Input text corpus organized by language
- **src/main/java/TTRJob.java**: Main Spark application
- **latex/**: Project documentation (`doku.tex`)
- **results/**: output CSV files


## How To Build & Run the Spark Job

### 1. Build JAR
```bash
mvn clean package
```

### 2. Run Spark Job
Submit the job to Spark (local mode):
```bash
spark-submit \
  --class TTRJob \
  --master "local[4]" \
  target/prog-alg-1.0.jar \
  data/text \
  data/stopwords \
  results
```

### 3. Output
Results will be printed to the console and saved to `results/ttr_results.csv`.


## Reproducibility & Benchmarks

To reproduce the performance analysis on different dataset sizes:

### 1. Python Setup
The benchmarking and graphing scripts require Python 3 and some libraries.

**Setup venv and install dependencies:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate Datasets
Run the provided scripts to create larger datasets by duplicating the original:
```bash
# Generate 'Large' dataset (~672 MB)
./scripts/generate_large_dataset.sh

# Generate 'Huge' dataset (~2.6 GB)
./scripts/generate_huge_dataset.sh
```

### 3. Run Automated Benchmarks
This script runs the Serial implementation vs. Spark (1, 2, 4, 8 cores) on all three datasets (Small, Medium, Huge):
```bash
python3 scripts/run_benchmarks.py
```
*Note: This may take 10-15 minutes depending on hardware.*

### 4. Generate Graphs
Visualize the results:
```bash
python3 scripts/create_graphs.py
```
Images will be saved to `latex/images/`.
