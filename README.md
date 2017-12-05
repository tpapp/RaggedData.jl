# RaggedData

[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](http://www.repostatus.org/badges/latest/wip.svg)](http://www.repostatus.org/#wip)
[![Build Status](https://travis-ci.org/tpapp/RaggedData.jl.svg?branch=master)](https://travis-ci.org/tpapp/RaggedData.jl)
[![Coverage Status](https://coveralls.io/repos/github/tpapp/RaggedData.jl/badge.svg?branch=master)](https://coveralls.io/github/tpapp/RaggedData.jl?branch=master)
[![codecov.io](http://codecov.io/github/tpapp/RaggedData.jl/coverage.svg?branch=master)](http://codecov.io/github/tpapp/RaggedData.jl?branch=master)

## Introduction

This package is for ingesting and working with ragged
(non-rectangular) data. To illustrate, let `1`, `2`, … denote
observations for individuals with a given index. A dataset may look
like this:

*file1*:
```
id,observation
1,obs11
1,obs12
1,obs13
2,obs21
3,obs31
```

*file2*:
```
id,observation
1,obs14
2,obs22
2,obs23
3,obs32
```

which has 4 observations for individual `1`, 3 for individual `2`, and
`2` for individual `3`.

## Ingestion

The type `RaggedCounter(T,S)` will help count keys of type `T` with
integers `::S` (eg `Int64`, though `Int32` should do for most datasets,
and may save memory if you are processing a lot of data). In the
first pass, you iterate through the data (possibly parsing and saving
it, see [LargeColumns.jl](https://github.com/tpapp/LargeColumns.jl)),
using `(::RaggedCounter)(id)` and you end up with the following
counts:

| id | count |
|----|-------|
| 1 | 4 |
| 2 | 3 |
| 3 | 2 |

Then for the second pass, you *collate* the records so that that
observations for the same individual are adjacent. Use
`collate_index_keys` to return a `collate::RaggedCollate` and an
`index::RaggedIndex` object, and possibly the `keys`, in the order
they were encountered.

The `RaggedCollate` object can keep track of indices for the second
pass: `next_index!` will return the next index for a given key.

## Ragged indices

A `RaggedIndex` object can be used to address ragged data packed flat
into a vector. For example, if the vector contains observations

```
111122233
```

Then the first ragged index would be `1:4`, the second `5:8`, the
third `9:10`.

## Ragged columns

`RaggedColumns` and `RaggedColumn` are thin wrapper types to address a
tuple of vectors using ragged indices.

# Acknowledgments

Work on this library was supported by the Austrian National Bank
Jubiläumsfonds grant #17378.
