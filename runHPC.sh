#!/usr/bin/env sh

ghc dist/build/cbits/wrapper.o -ldb -isrc -idist/build --make -O2 -fhpc src/Tests.hs &&
rm -f Tests.tix &&
./src/Tests &&
hpc markup Tests.tix
