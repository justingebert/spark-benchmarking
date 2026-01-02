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

## How To Build & Run

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
