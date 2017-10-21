module RaggedData

using AutoHashEquals
using Parameters
using Lazy

import Base:
    push!, count, sizehint!,
    length, size, indices, getindex, eltype, ndims, endof

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
`push!(rc::RaggedCounter, key)`. The order of new keys is preserved.

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

function push!(rc::RaggedCounter{T,S}, key) where {T,S}
    @unpack dict = rc
    if haskey(dict, key)
        dict[key].count += one(S)
    else
        dict[key] = RaggedCount(S(length(dict)) + one(S), one(S))
    end
    nothing
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
@auto_hash_equals struct RaggedIndex{S}
    cumsum::Vector{S}
end

@forward RaggedIndex.cumsum length, size, indices

count(ri::RaggedIndex) = ri.cumsum[end]

function getindex(ri::RaggedIndex{S}, i) where S
    UnitRange{S}(one(S) + (i == 1 ? zero(S) : ri.cumsum[i-1]), ri.cumsum[i])
end

eltype(ri::RaggedIndex{S}) where S = UnitRange{S}

"""

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

@auto_hash_equals struct RaggedColumns{Tix <: RaggedIndex, Tcolumns <: Tuple}
    ix::Tix
    columns::Tcolumns
end

@auto_hash_equals struct RaggedColumn{Tix <: RaggedIndex,
                                      Tcolumn <: AbstractVector}
    ix::Tix
    column::Tcolumn
end

@forward RaggedColumns.ix count, length

@forward RaggedColumn.ix count, length

ndims(::RaggedColumns) = 2

ndims(::RaggedColumn) = 1

size(A::RaggedColumns) = (length(A.ix), length(A.columns))

size(A::RaggedColumn) = (length(A.ix),)

indices(A::RaggedColumns) = (indices(A.ix, 1), Base.OneTo(length(A.columns)))

indices(A::RaggedColumn) = (indices(A.ix, 1), )

function size(A::RaggedColumns, i)
    if i == 1
        length(A.ix)
    elseif i == 2
        length(A.columns)
    else
        throw(ArgumentError("invalid dimension $i"))
    end
end

function size(A::RaggedColumn, i)
    i==1 ? length(A.ix) : throw(ArgumentError("invalid dimension $i"))
end

function getindex(A::RaggedColumns, i::Int)
    j = A.ix[i]
    map(v -> @view(v[j]), A.columns)
end

function getindex(A::RaggedColumn, i::Int)
    j = A.ix[i]
    @view(A.column[j])
end

getindex(A::RaggedColumns, i::Colon) = A

getindex(A::RaggedColumn, i::Colon) = A

endof(A::RaggedColumn) = length(A)

function getindex(A::RaggedColumns, I)
    sub_ix, sub_I = _subset(A.ix, to_indices(A.ix, (I,))...)
    RaggedColumns(sub_ix, map(v -> v[sub_I], A.columns))
end

function getindex(A::RaggedColumn, I)
    sub_ix, sub_I = _subset(A.ix, to_indices(A.ix, (I,))...)
    RaggedColumn(sub_ix, A.column[sub_I])
end

getindex(A::RaggedColumns, I1, I2::Int) =
    RaggedColumn(A.ix, A.columns[I2])[I1]

getindex(A::RaggedColumns, I1, I2::AbstractVector) =
    RaggedColumns(A.ix, A.columns[I2])[I1]

getindex(A::RaggedColumns, I1, I2) = A[to_indices(A, (I1, I2))...]

end # module
