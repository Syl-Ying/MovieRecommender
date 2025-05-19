#!/bin/bash

# ===== CONFIG =====
JAR_NAME="moviesim.jar"
SRC_DIR="."                     # Java source files location
OUT_DIR="classes"              # Compiled classes will go here
MAIN_CLASS="MovieSimilarities" # Your main class name

# ===== CLEAN UP OLD FILES =====
echo "🧹 Cleaning previous builds..."
rm -rf $OUT_DIR $JAR_NAME
mkdir -p $OUT_DIR

# ===== COMPILE =====
echo "🛠️  Compiling Java files..."
javac -classpath "$(hadoop classpath)" -d $OUT_DIR $SRC_DIR/*.java

if [ $? -ne 0 ]; then
    echo "❌ Compilation failed."
    exit 1
fi

# ===== PACKAGE TO JAR =====
echo "📦 Packaging into JAR..."
jar cfe $JAR_NAME $MAIN_CLASS -C $OUT_DIR .

if [ $? -eq 0 ]; then
    echo "✅ Build successful: $JAR_NAME"
else
    echo "❌ Failed to create JAR."
    exit 2
fi