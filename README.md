akka-pagerank
=============

Experimental page rank implementation in Akka

  - Graph modeling and loading
  - Naive PageRank implementation [http://ilpubs.stanford.edu:8090/386/]

### Running ###

See `me.juhanlol.akka.pagerank.Main` for details.
Notice that in some system, need to add

  `-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeRefBLAS`

as a VM parameter.
