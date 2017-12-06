__precompile__()
module RaggedData

using AutoHashEquals: @auto_hash_equals
using Parameters: @unpack
using Lazy: @forward

import Base:
    count, sizehint!, indices, length, size, getindex, IndexStyle

export
    RaggedCounter, collate_index_keys,
    RaggedCollate, next_index!,
    RaggedIndex, RaggedColumns, RaggedColumn

"""
    RaggedCount(index, count)

An element in a `RaggedCounter`. `index` allows ordering of the elements as they
are encountered, `count` keeps track of the number of elements so far in the
data for a particular key.
"""
@auto_hash_equals mutable struct RaggedCount{S <: Integer}
    index::S
    count::S
end

"""
    RaggedCounter(T, S)

Count keys of type `T` with integers of type `S`. New keys can be added with
`(rc::RaggedCounter(key)`, which returns `key`. The order of new keys is
preserved.

`count(::RaggedCounter)` returns the total number of keys counter, while
`length(::RaggedCounter)` the number of unique keys. Use `sizehint!` for a
slight speed increase if you can estimate the number of elements in advance.

## Recommended usage

Use in the *first pass* of ingesting the data to count the number each key
occurs. Then generate a collator and indexer using `collate_index_keys` for the
*second pass* and then indexing.
"""
@auto_hash_equals struct RaggedCounter{T,S}
    dict::Dict{T,RaggedCount{S}}
end

RaggedCounter(T,S::Type{<:Integer}) =
    RaggedCounter{T,S}(Dict{T,RaggedCount{S}}())

function (rc::RaggedCounter{T,S})(key) where {T,S}
    @unpack dict = rc
    if haskey(dict, key)
        dict[key].count += one(S)
    else
        dict[key] = RaggedCount(S(length(dict)) + one(S), one(S))
    end
    key
end

count(rc::RaggedCounter) = sum(v.count for v in values(rc.dict))

@forward RaggedCounter.dict length, sizehint!

"""
    RaggedCollate

Use in the *second pass* of reading the data to return increasing indices
(`nextindex!`) that form an ordered partition of `1:count` after the whole data
is traversed.
"""
@auto_hash_equals struct RaggedCollate{T,S}
    dict::Dict{T,S}
    total::S
end

next_index!(coll::RaggedCollate{T,S}, key) where {T,S} = coll.dict[key] += one(S)

"""
    RaggedIndex(count_cumsum)

Create an index that maps key indices (with given counts) to indices of a flat
vector. See `collate_index_keys` for efficient creation from `RaggedCounter`s.

Example:
```jldoctest
julia> ri = RaggedIndex(cumsum(1:3))
RaggedData.RaggedIndex{Int64}([1, 3, 6])

julia> ri[1]
1:1

julia> ri[2]
2:3

julia> ri[3]
4:6
```
"""
@auto_hash_equals struct RaggedIndex{S} <: AbstractVector{UnitRange{S}}
    cumsum::Vector{S}
end

@forward RaggedIndex.cumsum length, size

count(ri::RaggedIndex) = ri.cumsum[end]

function getindex(ri::RaggedIndex{S}, i) where S
    UnitRange{S}(one(S) + (i == 1 ? zero(S) : ri.cumsum[i-1]), ri.cumsum[i])
end

"""
    sub_ix, sub_I = _subset(ix::RaggedIndex, I)

Calculate a `RangeIndex` and corresponding set of `UnitRange` indices such that
indexing the original is possible for the subset, ie

```julia
v[I][sub_ix[j]] == v[sub_I[j]]
```

∀ j ∈ indices(v).

Useful for subsets of `RaggedColumn` and `RaggedColumns`.
"""
function _subset(ix::RaggedIndex{S}, I) where {S}
    partial_sum = zero(S)
    sub_cumsum = similar(Array{S}, indices(I))
    sub_I = Vector{S}()
    for (j,i) in enumerate(I)
        k = ix[i]
        append!(sub_I, k)
        partial_sum += length(k)
        sub_cumsum[j] = partial_sum
    end
    RaggedIndex(sub_cumsum), sub_I
end

"""
    collate, index, keys = collate_index_keys(ragged_counter; collect_keys = false)

Return a collator `collate`, an index `index`, and possibly `keys` (may be an
empty vector if `!collect_keys`.
"""
function collate_index_keys(rc::RaggedCounter{T,S},
                            collect_keys = false) where {T,S}
    kv = collect(rc.dict)
    sort!(kv, by = x -> x.second.index)
    partial_sum = zero(S)
    dict = Dict{T, S}()
    sizehint!(dict) = length(kv)
    index = similar(kv, S)
    keys = ifelse(collect_keys, similar(kv, T), Vector{T}(0))
    @inbounds for i in indices(kv, 1) # dict, cumsum and keys in one sweep
        key = kv[i].first
        dict[key] = partial_sum
        partial_sum += kv[i].second.count
        index[i] = partial_sum
        if collect_keys
            keys[i] = key
        end
    end
    RaggedCollate(dict, partial_sum), RaggedIndex(index), keys
end

_vector_view_type(S,T) = SubArray{T,1,Array{T,1},Tuple{UnitRange{S}},true}

_vector_view_type(::RaggedIndex{S}, ::AbstractVector{T}) where {S, T} =
    _vector_view_type(S, T)

"""
    RaggedColumns(count_cumsum, columns)

A tuple of columns (vectors) indexes by a `RaggedIndex`.

`getindex(::RaggedColumns, ::Int)` returns a tuple of vectors for that index.

`getindex(::RaggedColumns, ::Any)` subsets the indexed vector.

Use `RaggedColumn(::RaggedColumns, i::Int)` or `RaggedColumns(::RaggedColumns, I)`
to obtain single or multiple columns.
"""
@auto_hash_equals struct RaggedColumns{Tix <: RaggedIndex,
                                       Tcolumns <: Tuple, S} <:
                                           AbstractVector{S}
    ix::Tix
    columns::Tcolumns
    function RaggedColumns(ix::Tix,
                           columns::Tcolumns) where {Tix <: RaggedIndex,
                                                     Tcolumns <:
                                                     Tuple{Vararg{AbstractVector}}}
        S = Tuple{map(c -> _vector_view_type(ix, c), columns)...}
        new{Tix, Tcolumns, S}(ix, columns)
    end
end

RaggedColumns(rc::RaggedColumns, I) = RaggedColumns(rc.ix, rc.columns[I])

RaggedColumns(rc::RaggedColumns, i::Int) =
    throw(ArgumentError("Use RaggedColumn(::RaggedColumns, ::Int) for a single column."))

@forward RaggedColumns.ix count, length

"""
    RaggedColumn(count_cumsum, column)

A single column indexed by a `RaggedIndex`.

`getindex(::RaggedColumn, ::Int)` returns a vector for that index.

`getindex(::RaggedColumn, ::Any)` subsets the indexed vector.
"""
@auto_hash_equals struct RaggedColumn{Tix <: RaggedIndex,
                                      Tcolumn <: AbstractVector, S} <:
                                          AbstractVector{S}
    ix::Tix
    column::Tcolumn
    function RaggedColumn(ix::Tix, column::Tcolumn) where {Tix <: RaggedIndex,
                                                           Tcolumn <: AbstractVector}
        S = _vector_view_type(ix, column)
        new{Tix, Tcolumn, S}(ix, column)
    end
end

RaggedColumn(rc::RaggedColumns, i::Int) = RaggedColumn(rc.ix, rc.columns[i])

@forward RaggedColumn.ix count, length

size(A::Union{RaggedColumn,RaggedColumns}) = (length(A.ix),)

IndexStyle(::Union{RaggedColumn,RaggedColumns}) = Base.IndexLinear()

function getindex(A::RaggedColumns, i::Int)
    j = A.ix[i]
    map(v -> @view(v[j]), A.columns)
end

function getindex(A::RaggedColumn, i::Int)
    j = A.ix[i]
    @view(A.column[j])
end

function getindex(A::RaggedColumns, I)
    sub_ix, sub_I = _subset(A.ix, to_indices(A.ix, (I,))...)
    RaggedColumns(sub_ix, map(v -> v[sub_I], A.columns))
end

function getindex(A::RaggedColumn, I)
    sub_ix, sub_I = _subset(A.ix, to_indices(A.ix, (I,))...)
    RaggedColumn(sub_ix, A.column[sub_I])
end

end # module
