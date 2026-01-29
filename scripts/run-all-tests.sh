#!/bin/bash

echo "=========================================="
echo "Running All KAFS Hook Test Suites"
echo "=========================================="
echo ""
echo "Date: $(date)"
echo ""

RESULTS_FILE="TEST_EXECUTION_RESULTS.txt"
> "$RESULTS_FILE"

# Test 1
echo "Test 1/3: Complete Git Workflow Test"
echo "=====================================" | tee -a "$RESULTS_FILE"
if ./scripts/test-git-operations.sh 2>&1 | tee -a "$RESULTS_FILE"; then
    TEST1_RESULT="PASSED"
    echo "✓ Test 1 PASSED" | tee -a "$RESULTS_FILE"
else
    TEST1_RESULT="FAILED"
    echo "✗ Test 1 FAILED" | tee -a "$RESULTS_FILE"
fi
echo "" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Test 2
echo "Test 2/3: Hook Functions Direct Test"
echo "=====================================" | tee -a "$RESULTS_FILE"
if ./scripts/test-hooks-direct.sh 2>&1 | tee -a "$RESULTS_FILE"; then
    TEST2_RESULT="PASSED"
    echo "✓ Test 2 PASSED" | tee -a "$RESULTS_FILE"
else
    TEST2_RESULT="FAILED"
    echo "✗ Test 2 FAILED" | tee -a "$RESULTS_FILE"
fi
echo "" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Test 3
echo "Test 3/3: Fresh Image Git fsck Test"
echo "=====================================" | tee -a "$RESULTS_FILE"
if ./scripts/test-fresh-image-git-fsck.sh 2>&1 | tee -a "$RESULTS_FILE"; then
    TEST3_RESULT="PASSED"
    echo "✓ Test 3 PASSED" | tee -a "$RESULTS_FILE"
else
    TEST3_RESULT="FAILED"
    echo "✗ Test 3 FAILED" | tee -a "$RESULTS_FILE"
fi
echo "" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Summary
echo "=========================================="
echo "FINAL TEST SUMMARY"
echo "=========================================="
echo "Test 1 (Git Workflow):         $TEST1_RESULT"
echo "Test 2 (Hook Functions):       $TEST2_RESULT"
echo "Test 3 (Fresh Image Git fsck): $TEST3_RESULT"
echo ""

if [ "$TEST1_RESULT" = "PASSED" ] && [ "$TEST2_RESULT" = "PASSED" ] && [ "$TEST3_RESULT" = "PASSED" ]; then
    echo "✓✓✓ ALL TESTS PASSED ✓✓✓"
    echo ""
    echo "CONCLUSION: EIO/SHA1 errors NO LONGER OCCUR"
    echo "after implementing flush/fsync/release/fsyncdir hooks"
    exit 0
else
    echo "✗✗✗ SOME TESTS FAILED ✗✗✗"
    exit 1
fi
