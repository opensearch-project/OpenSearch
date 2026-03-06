/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms;

import org.opensearch.protobufs.StringArray;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.protobufs.TermsPartition;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for IncludeExcludeProtoUtils.
 * Tests verify that gRPC proto conversion matches REST API behavior from IncludeExclude.java
 */
public class IncludeExcludeProtoUtilsTests extends OpenSearchTestCase {

    // ========================================
    // parseInclude() tests - TERMS case (array format)
    // ========================================

    public void testParseIncludeWithTermsArray() {
        StringArray terms = StringArray.newBuilder()
            .addStringArray("term1")
            .addStringArray("term2")
            .addStringArray("term3")
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setTerms(terms)
            .build();

        IncludeExclude result = IncludeExcludeProtoUtils.parseInclude(includeProto);

        assertNotNull(result);
        // IncludeExclude doesn't expose the include set directly, but we can verify it's not null
        // and doesn't throw when used
    }

    public void testParseIncludeWithEmptyTermsArray() {
        // Empty array should still return IncludeExclude object (matching REST behavior)
        StringArray terms = StringArray.newBuilder().build();  // Empty array

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setTerms(terms)
            .build();

        IncludeExclude result = IncludeExcludeProtoUtils.parseInclude(includeProto);

        assertNotNull("Empty array should return IncludeExclude object, not null", result);
    }

    public void testParseIncludeWithSingleTerm() {
        StringArray terms = StringArray.newBuilder()
            .addStringArray("single_term")
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setTerms(terms)
            .build();

        IncludeExclude result = IncludeExcludeProtoUtils.parseInclude(includeProto);

        assertNotNull(result);
    }

    // ========================================
    // parseInclude() tests - PARTITION case
    // ========================================

    public void testParseIncludeWithValidPartition() {
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(0)
            .setNumPartitions(10)
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        IncludeExclude result = IncludeExcludeProtoUtils.parseInclude(includeProto);

        assertNotNull(result);
    }

    public void testParseIncludeWithPartitionZero() {
        // Partition 0 is valid (first partition)
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(0)
            .setNumPartitions(5)
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        IncludeExclude result = IncludeExcludeProtoUtils.parseInclude(includeProto);

        assertNotNull(result);
    }

    public void testParseIncludeWithPartitionLastIndex() {
        // partition=9, num_partitions=10 is valid (last partition)
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(9)
            .setNumPartitions(10)
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        IncludeExclude result = IncludeExcludeProtoUtils.parseInclude(includeProto);

        assertNotNull(result);
    }

    public void testParseIncludeThrowsWhenNumPartitionsIsZero() {
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(0)
            .setNumPartitions(0)  // Invalid: must be > 0
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IncludeExcludeProtoUtils.parseInclude(includeProto)
        );
        assertTrue(ex.getMessage().contains("num_partitions"));
    }

    public void testParseIncludeThrowsWhenNumPartitionsIsNegative() {
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(0)
            .setNumPartitions(-5)  // Invalid: must be > 0
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IncludeExcludeProtoUtils.parseInclude(includeProto)
        );
        assertTrue(ex.getMessage().contains("num_partitions"));
    }

    public void testParseIncludeThrowsWhenPartitionIsNegative() {
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(-1)  // Invalid: must be >= 0
            .setNumPartitions(10)
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IncludeExcludeProtoUtils.parseInclude(includeProto)
        );
        assertTrue(ex.getMessage().contains("partition"));
    }

    // ========================================
    // parseInclude() tests - TERMSINCLUDE_NOT_SET case
    // ========================================

    public void testParseIncludeThrowsWhenTermsIncludeNotSet() {
        // Create a TermsInclude with no oneof case set
        TermsInclude includeProto = TermsInclude.newBuilder().build();

        // Should throw exception (matching REST behavior where empty object throws)
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IncludeExcludeProtoUtils.parseInclude(includeProto)
        );
        assertTrue(ex.getMessage().contains("Unrecognized token"));
        assertTrue(ex.getMessage().contains("include"));
    }

    // ========================================
    // parseExclude() tests
    // ========================================

    public void testParseExcludeWithTermsList() {
        List<String> excludeList = Arrays.asList("term1", "term2", "term3");

        IncludeExclude result = IncludeExcludeProtoUtils.parseExclude(excludeList);

        assertNotNull(result);
    }

    public void testParseExcludeWithSingleTerm() {
        List<String> excludeList = Collections.singletonList("excluded_term");

        IncludeExclude result = IncludeExcludeProtoUtils.parseExclude(excludeList);

        assertNotNull(result);
    }

    public void testParseExcludeWithEmptyList() {
        // Empty list should still return IncludeExclude object (matching REST behavior)
        List<String> excludeList = Collections.emptyList();

        IncludeExclude result = IncludeExcludeProtoUtils.parseExclude(excludeList);

        assertNotNull("Empty list should return IncludeExclude object, not null", result);
    }

    // ========================================
    // Integration tests - Merge behavior
    // ========================================

    public void testParseIncludeAndExcludeMerge() {
        // Test that parseInclude and parseExclude can be merged (like REST does)
        StringArray includeTerms = StringArray.newBuilder()
            .addStringArray("include1")
            .addStringArray("include2")
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setTerms(includeTerms)
            .build();

        List<String> excludeList = Arrays.asList("exclude1", "exclude2");

        IncludeExclude include = IncludeExcludeProtoUtils.parseInclude(includeProto);
        IncludeExclude exclude = IncludeExcludeProtoUtils.parseExclude(excludeList);

        // Both should be non-null and mergeable using IncludeExclude.merge()
        assertNotNull(include);
        assertNotNull(exclude);

        IncludeExclude merged = IncludeExclude.merge(include, exclude);
        assertNotNull(merged);
    }

    public void testParseIncludeWithPartitionCanBeMerged() {
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(2)
            .setNumPartitions(10)
            .build();

        TermsInclude includeProto = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        IncludeExclude include = IncludeExcludeProtoUtils.parseInclude(includeProto);

        assertNotNull(include);

        // Verify it can be merged with null (simulating builder.includeExclude() being null)
        IncludeExclude merged = IncludeExclude.merge(include, null);
        assertNotNull(merged);
    }
}
