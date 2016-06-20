#!/bin/bash

BASE=$(realpath .)
TEST_D=$BASE/tests
SRC_D=$BASE/src
TESTS=$(ls $TEST_D)
TOTAL=0
PASSED=0

check_test () {
    if [[ -n `grep 'Segmentation fault' $1` ]]; then
        echo "Segfault"
    elif [[ -n `grep FAIL $1` ]]; then
        echo "Fail"
    elif [[ -n `grep DONE $1` ]]; then
        echo "Ok"
        PASSED=$[PASSED + 1]
    else
        echo "Didn't finish"
    fi
}

cd _build/src
echo "Running tests:"

for t in $TESTS; do
    TOTAL=$[TOTAL + 1]
    printf "$t "
    ocamlfind ocamlopt -o $TEST_D/_bin_$t -thread -linkpkg -package ctypes zkocaml.cmxa -cclib -L. $TEST_D/$t
    OUT=$TEST_D/_out_$t
    $TEST_D/_bin_$t &> $OUT
    check_test $OUT
done;

cd ../..
echo "Passed: $PASSED/$TOTAL"
