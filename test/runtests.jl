using RaggedData
using Base.Test

import RaggedData: _subset

@testset "ragged data processing" begin
    # initialize and countr
    rc = RaggedCounter(Int,Int32)
    push!(rc, 3)
    push!(rc, 2)
    push!(rc, 1)
    for _ in 1:3
        push!(rc, 1)
    end
    for _ in 1:10
        push!(rc, 2)
    end
    for _ in 1:7
        push!(rc, 3)
    end
    @test length(rc) == 3
    @test count(rc) == 3+3+10+7
    # test index and collation
    coll, ix, ks = collate_index_keys(rc, true)
    @test ks == Int32[3, 2, 1]
    # index
    @test length(ix) ≡ 3
    @test size(ix) ≡ (3,)
    @test eltype(ix) ≡ UnitRange{Int32}
    @test ix[1] ≡ UnitRange{Int32}(1, 8)
    @test ix[2] ≡ UnitRange{Int32}(9, 19)
    @test ix[3] ≡ UnitRange{Int32}(20, 23)
    @test indices(ix) ≡ indices(ones(3))
    # collation
    @test next_index!(coll, 3) == 1
    for i in 9:15
        @test next_index!(coll, 2) == i
    end
    for i in 20:23
        @test next_index!(coll, 1) == i
    end
    for i in 16:19
        @test next_index!(coll, 2) == i
    end
    for i in 2:8
        @test next_index!(coll, 3) == i
    end
    # equality testing
    @test isequal(rc, deepcopy(rc))
    @test isequal(coll, deepcopy(coll))
    @test isequal(ix, deepcopy(ix))
end

@testset "ragged index subset calculations" begin
    ix = RaggedIndex(cumsum([5,3,2,8]))
    sub_ix, sub_I = _subset(ix, 3:4)
    @test sub_ix == RaggedIndex(cumsum([2,8]))
    @test sub_I == collect(9:18)
end

@testset "ragged columns" begin
    ix = RaggedIndex(cumsum([5,3,2,7]))
    v1 = collect(1:17)
    v2 = collect(StepRangeLen(18.0, 1.0, 17))
    rc = RaggedColumns(ix, (v1, v2))
    @test length(rc) == 4
    @test count(rc) == 17
    for i in 1:length(rc)
        j = ix[i]
        @test rc[i] == (v1[j], v2[j])
    end
    @test rc[:, 2] == RaggedColumn(ix, v2)
    @test rc[2, 1] == v1[ix[2]]
    @test rc[:, 1:1] == RaggedColumns(ix, (v1,))
    @test rc[2, end:end] == (v2[ix[2]],)
    @test rc[3:4, 1:2] == RaggedColumns(RaggedIndex(cumsum([2,7])), (v1[9:17], v2[9:17]))
    @test rc[3:4, :] == RaggedColumns(RaggedIndex(cumsum([2,7])), (v1[9:17], v2[9:17]))
    @test rc[:, :] == rc
    rc1 = rc[:, 1]
    @test rc1[3:4] == RaggedColumn(RaggedIndex(cumsum([2,7])), v1[9:17])
    @test rc1[:] == rc1
    @test rc1[end] == v1[11:17]

    @test_throws ArgumentError size(rc1, 2)
    @test_throws ArgumentError size(rc1, 0)
    @test_throws ArgumentError size(rc, 3)
    @test_throws ArgumentError size(rc, 0)

    @test ndims(rc1) == 1
    @test ndims(rc) == 2
    @test size(rc1) == (4,)
    @test size(rc) == (4, 2)
    @test size(rc, 1) == 4
end
