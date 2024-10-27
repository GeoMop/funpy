#!/bin/bash

set -x
which python

# Generate reference data for actual python environment
COMPRESS_METHODS="zlib gzip bz2 xz lzma lz4"

# install funpy
pip install ../../..

# Generate non compressed pickles.
python create_numpy_pickle.py

# Generate compressed pickles for each compression methods supported
for method in $COMPRESS_METHODS
do
    python create_numpy_pickle.py --compress --method $method
done
