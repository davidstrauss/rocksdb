#!/bin/sh
export CXX=clang++
export CC=clang
make static_lib test_libs ttl_test
./ttl_test
