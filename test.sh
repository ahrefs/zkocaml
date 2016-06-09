#!/bin/bash

TEST_D=$(realpath tests)
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
    OUT=$TEST_D/_out_$t
    cat "$TEST_D/$t" | ocaml &> $OUT
    check_test $OUT
done;

cd ../..
echo "Passed: $PASSED/$TOTAL"
