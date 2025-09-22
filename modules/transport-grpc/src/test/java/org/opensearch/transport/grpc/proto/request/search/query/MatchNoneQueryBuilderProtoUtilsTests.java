/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.protobufs.MatchNoneQuery;
import org.opensearch.test.OpenSearchTestCase;

public class MatchNoneQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithDefaultValues() {
        // Create a protobuf MatchNoneQuery with default values
        MatchNoneQuery matchNoneQueryProto = MatchNoneQuery.newBuilder().build();

        // Call the method under test
        MatchNoneQueryBuilder matchNoneQueryBuilder = MatchNoneQueryBuilderProtoUtils.fromProto(matchNoneQueryProto);

        // Verify the result
        assertNotNull("MatchNoneQueryBuilder should not be null", matchNoneQueryBuilder);
        assertEquals("Boost should be default", 1.0f, matchNoneQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", matchNoneQueryBuilder.queryName());
    }

    public void testFromProtoWithBoost() {
        // Create a protobuf MatchNoneQuery with boost
        MatchNoneQuery matchNoneQueryProto = MatchNoneQuery.newBuilder().setBoost(2.5f).build();

        // Call the method under test
        MatchNoneQueryBuilder matchNoneQueryBuilder = MatchNoneQueryBuilderProtoUtils.fromProto(matchNoneQueryProto);

        // Verify the result
        assertNotNull("MatchNoneQueryBuilder should not be null", matchNoneQueryBuilder);
        assertEquals("Boost should match", 2.5f, matchNoneQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", matchNoneQueryBuilder.queryName());
    }

    public void testFromProtoWithName() {
        // Create a protobuf MatchNoneQuery with name
        MatchNoneQuery matchNoneQueryProto = MatchNoneQuery.newBuilder().setXName("test_query").build();

        // Call the method under test
        MatchNoneQueryBuilder matchNoneQueryBuilder = MatchNoneQueryBuilderProtoUtils.fromProto(matchNoneQueryProto);

        // Verify the result
        assertNotNull("MatchNoneQueryBuilder should not be null", matchNoneQueryBuilder);
        assertEquals("Boost should be default", 1.0f, matchNoneQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchNoneQueryBuilder.queryName());
    }

    public void testFromProtoWithBoostAndName() {
        // Create a protobuf MatchNoneQuery with boost and name
        MatchNoneQuery matchNoneQueryProto = MatchNoneQuery.newBuilder().setBoost(3.0f).setXName("test_query").build();

        // Call the method under test
        MatchNoneQueryBuilder matchNoneQueryBuilder = MatchNoneQueryBuilderProtoUtils.fromProto(matchNoneQueryProto);

        // Verify the result
        assertNotNull("MatchNoneQueryBuilder should not be null", matchNoneQueryBuilder);
        assertEquals("Boost should match", 3.0f, matchNoneQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchNoneQueryBuilder.queryName());
    }

    public void testFromProtoWithNullInput() {
        // Call the method under test with null input, should throw NullPointerException
        NullPointerException exception = expectThrows(NullPointerException.class, () -> MatchNoneQueryBuilderProtoUtils.fromProto(null));
    }
}
