import polars as pl
import polars.datatypes as pld

import pyarrow as pa
import pyarrow.parquet as pq

# base path for the output parquet files
BASE="result"

def i8(backend):
    match backend:
        case "polars": return pld.Int8
        case "pyarrow": return pa.int8()
        case _: raise Exception("Unknown backend")

def varlist(inner):
    def _varlist(backend):
        match backend:
            case "polars": return pld.List(inner(backend))
            case "pyarrow": return pa.list_(inner(backend))
            case _: raise Exception("Unknown backend")

    return _varlist

def fixedlist(inner, width):
    def _fixedlist(backend):
        match backend:
            case "polars": return pld.Array(inner(backend), width)
            case "pyarrow": return pa.list_(inner(backend), width)
            case _: raise Exception("Unknown backend")

    return _fixedlist

columns = dict(
    primitives       = ([1, 2, 3, 7, 5, 4],                            i8),
    primitives_null  = ([1, 2, 3, None, 5, 4],                         i8),
    basic_list       = ([[1], [2, 3], [3], [], [5], [4]],     varlist(i8)),
    null_list        = ([[1], [2, 3], None, [], [5], [4]],    varlist(i8)),
    nested_null_list = ([[1], [None, 3], None, [], [5], [4]], varlist(i8)),
)

# Polars
frame = {}
for key, (array, dtype) in columns.items():
    frame[key] = pl.Series(array, dtype=dtype("polars"))

pl.DataFrame(frame).write_parquet(f"{BASE}/polars.parquet")

# PyArrow
arrays = []
names = []
for key, (array, dtype) in columns.items():
    arrays.append(pa.array(array, type=dtype("pyarrow")))
    names.append(key)

t = pa.table(arrays, names=names)
pq.write_table(t, f"{BASE}/pyarrow.parquet")
