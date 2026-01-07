#!/bin/bash
# Script to generate a Huge dataset (~2GB) by duplicating text_large

echo "Generating Huge Dataset (~2GB)..."

SOURCE="data/text_large"
DEST="data/text_huge"

if [ ! -d "$SOURCE" ]; then
    echo "Error: Source $SOURCE does not exist."
    exit 1
fi

rm -rf "$DEST"
mkdir -p "$DEST"

# Copy structure and files 3 times
for i in {1..4}; do  # Increased to 4 passes to ensure ~2.5GB
    echo "Copying pass $i/4..."
    
    # Iterate over language directories in source
    for lang_dir in "$SOURCE"/*; do
        if [ -d "$lang_dir" ]; then
            lang_name=$(basename "$lang_dir")
            mkdir -p "$DEST/$lang_name"
            
            # Copy files with suffix
            for file in "$lang_dir"/*.txt; do
                 if [ -f "$file" ]; then
                     filename=$(basename "$file")
                     # format: originalname_batch1.txt
                     cp "$file" "$DEST/$lang_name/${filename%.*}_batch${i}.txt"
                 fi
            done
        fi
    done
done

echo "Huge dataset generated in $DEST"
du -sh "$DEST"
