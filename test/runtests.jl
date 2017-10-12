using RaggedData
using Base.Test

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
end
