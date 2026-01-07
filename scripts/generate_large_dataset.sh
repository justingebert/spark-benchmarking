#!/bin/bash
# Script to generate a Large dataset (~672MB) by duplicating text 5 times

echo "Generating Large Dataset (~672MB)..."

SOURCE="data/text"
DEST="data/text_large"

if [ ! -d "$SOURCE" ]; then
    echo "Error: Source $SOURCE does not exist. Please ensure data/text contains the original dataset."
    exit 1
fi

rm -rf "$DEST"
mkdir -p "$DEST"

# Copy structure and files 5 times
for i in {1..5}; do
    echo "Copying pass $i/5..."
    
    # Iterate over language directories in source
    for lang_dir in "$SOURCE"/*; do
        if [ -d "$lang_dir" ]; then
            lang_name=$(basename "$lang_dir")
            mkdir -p "$DEST/$lang_name"
            
            # Copy files with suffix to ensure unique filenames and flat structure per language
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

echo "Large dataset generated in $DEST"
du -sh "$DEST"
