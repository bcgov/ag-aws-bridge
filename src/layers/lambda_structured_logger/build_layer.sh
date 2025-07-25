# ==================================================
# FILE: build_layer.sh
# ==================================================

#!/bin/bash

# Build script for Lambda Layer
# Run this to create the layer zip file

set -e

echo "Building Lambda Structured Logger Layer..."

# Create build directory
BUILD_DIR="build"
LAYER_DIR="$BUILD_DIR/lambda-structured-logger-layer"

# Clean previous build
rm -rf $BUILD_DIR
mkdir -p $LAYER_DIR

# Copy the python package
cp -r python $LAYER_DIR/

# Create the zip file
cd $BUILD_DIR
zip -r ../lambda-structured-logger-layer.zip .
cd ..

echo "Layer built successfully: lambda-structured-logger-layer.zip"
echo "Upload this zip file to AWS Lambda Layers"