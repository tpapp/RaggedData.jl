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

    # columns
    rc = RaggedColumns(ix, (v1, v2))
    @test length(rc) == 4
    @test count(rc) == 17
    @test eltype(rc) == typeof(((@view v1[1:1]), (@view v2[1:1]))) == typeof(rc[1])
    @test size(rc) == (4,)
    @test size(rc, 1) == 4
    for i in indices(rc, 1)
        j = ix[i]
        @test rc[i] == (v1[j], v2[j])
    end
    @test rc[3:4] == RaggedColumns(RaggedIndex(cumsum([2,7])), (v1[9:17], v2[9:17]))
    @test collect(rc) ==
        [([1, 2, 3, 4, 5], [18.0, 19.0, 20.0, 21.0, 22.0]),
         ([6, 7, 8], [23.0, 24.0, 25.0]),
         ([9, 10], [26.0, 27.0]),
         ([11, 12, 13, 14, 15, 16, 17], [28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0])]

    # subset of columns
    rc1 = RaggedColumns(rc, [1])
    @test length(rc1) == 4
    @test count(rc1) == 17
    @test eltype(rc1) == typeof(((@view v1[1:1]),)) == typeof(rc1[1])
    @test size(rc1) == (4,)
    @test size(rc1, 1) == 4
    for i in indices(rc1, 1)
        j = ix[i]
        @test rc1[i] == (v1[j],)
    end
    @test_throws ArgumentError RaggedColumns(rc, 1)
    @test collect(rc1) ==
        [([1, 2, 3, 4, 5], ),
         ([6, 7, 8], ),
         ([9, 10], ),
         ([11, 12, 13, 14, 15, 16, 17], )]

    # single column
    rc2 = RaggedColumn(rc, 1)
    @test length(rc2) == 4
    @test count(rc2) == 17
    @test eltype(rc2) == typeof(@view v1[1:1]) == typeof(rc2[1])
    @test size(rc2) == (4,)
    @test size(rc2, 1) == 4
    for i in indices(rc2, 1)
        j = ix[i]
        @test rc2[i] == v1[j]
    end
    @test_throws MethodError RaggedColumn(rc, 1:1)
    @test collect(rc2) ==
        Any[[1, 2, 3, 4, 5], [6, 7, 8], [9, 10], [11, 12, 13, 14, 15, 16, 17]]
end
