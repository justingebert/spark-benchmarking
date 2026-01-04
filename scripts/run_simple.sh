#!/bin/bash

# 1. Build
echo "Building..."
mvn clean package -q

# 2. Run with different cores
echo "Running with 1 Core..."
spark-submit --class TTRJob --master "local[1]" target/prog-alg-1.0.jar data/text_large data/stopwords results > results_1core.txt

echo "Running with 4 Cores..."
spark-submit --class TTRJob --master "local[4]" target/prog-alg-1.0.jar data/text_large data/stopwords results > results_4core.txt