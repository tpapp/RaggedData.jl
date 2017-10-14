Pkg.clone(pwd())
# Pkg.checkout("Lazy")
Pkg.clone("https://github.com/afniedermayer/InferenceUtilities.jl.git") # until registered
Pkg.build("RaggedData")
Pkg.test("RaggedData"; coverage=true)
