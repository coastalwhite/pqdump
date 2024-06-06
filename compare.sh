#!/bin/sh

set -e

python3 generate-parquet.py

cargo build

./target/debug/pqdump result/polars.parquet > /tmp/.pqdump.polars 
./target/debug/pqdump result/pyarrow.parquet > /tmp/.pqdump.pyarrow

if [ -x "$(which delta)" ] ; then
    delta /tmp/.pqdump.polars /tmp/.pqdump.pyarrow
else
    diff /tmp/.pqdump.polars /tmp/.pqdump.pyarrow
fi