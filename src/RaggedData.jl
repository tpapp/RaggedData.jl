module RaggedData

using AutoHashEquals
using Parameters
using Lazy

import Base: length, size, indices, push!, count, sizehint!, getindex, eltype

export RaggedCounter, RaggedCollate, next_index!, RaggedIndex, collate_index_keys

@auto_hash_equals mutable struct RaggedCount{S <: Integer}
    index::S
    count::S
end

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

@auto_hash_equals struct RaggedCollate{T,S}
    dict::Dict{T,S}
    total::S
end

next_index!(coll::RaggedCollate{T,S}, key) where {T,S} = coll.dict[key] += one(S)

@auto_hash_equals struct RaggedIndex{S}
    cumsum::Vector{S}
end

@forward RaggedIndex.cumsum length, size, indices

function getindex(ri::RaggedIndex{S}, i) where S
    UnitRange{S}(i == 1 ? one(S) : (ri.cumsum[i-1]+one(S)), ri.cumsum[i])
end

eltype(ri::RaggedIndex{S}) where S = UnitRange{S}

function collate_index_keys(rc::RaggedCounter{T,S},
                            collect_keys = false) where {T,S}
    kv = collect(rc.dict)
    sort!(kv, by = x -> x.second.index)
    partial_sum = zero(S)
    dict = Dict{T, S}()
    sizehint!(dict) = length(kv)
    index = similar(kv, S)
    keys = ifelse(collect_keys, similar(kv, T), Vector{T}(0))
    @inbounds for i in indices(kv, 1)
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

end # module
