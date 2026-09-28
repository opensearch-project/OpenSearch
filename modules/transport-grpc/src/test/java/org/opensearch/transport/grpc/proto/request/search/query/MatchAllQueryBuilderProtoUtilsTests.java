/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.test.OpenSearchTestCase;

public class MatchAllQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithDefaultValues() {
        // Create a protobuf MatchAllQuery with default values
        MatchAllQuery matchAllQueryProto = MatchAllQuery.newBuilder().build();

        // Call the method under test
        MatchAllQueryBuilder matchAllQueryBuilder = MatchAllQueryBuilderProtoUtils.fromProto(matchAllQueryProto);

        // Verify the result
        assertNotNull("MatchAllQueryBuilder should not be null", matchAllQueryBuilder);
        assertEquals("Boost should be default", 1.0f, matchAllQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", matchAllQueryBuilder.queryName());
    }

    public void testFromProtoWithBoost() {
        // Create a protobuf MatchAllQuery with boost
        MatchAllQuery matchAllQueryProto = MatchAllQuery.newBuilder().setBoost(2.5f).build();

        // Call the method under test
        MatchAllQueryBuilder matchAllQueryBuilder = MatchAllQueryBuilderProtoUtils.fromProto(matchAllQueryProto);

        // Verify the result
        assertNotNull("MatchAllQueryBuilder should not be null", matchAllQueryBuilder);
        assertEquals("Boost should match", 2.5f, matchAllQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", matchAllQueryBuilder.queryName());
    }

    public void testFromProtoWithName() {
        // Create a protobuf MatchAllQuery with name
        MatchAllQuery matchAllQueryProto = MatchAllQuery.newBuilder().setXName("test_query").build();

        // Call the method under test
        MatchAllQueryBuilder matchAllQueryBuilder = MatchAllQueryBuilderProtoUtils.fromProto(matchAllQueryProto);

        // Verify the result
        assertNotNull("MatchAllQueryBuilder should not be null", matchAllQueryBuilder);
        assertEquals("Boost should be default", 1.0f, matchAllQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchAllQueryBuilder.queryName());
    }

    public void testFromProtoWithBoostAndName() {
        // Create a protobuf MatchAllQuery with boost and name
        MatchAllQuery matchAllQueryProto = MatchAllQuery.newBuilder().setBoost(3.0f).setXName("test_query").build();

        // Call the method under test
        MatchAllQueryBuilder matchAllQueryBuilder = MatchAllQueryBuilderProtoUtils.fromProto(matchAllQueryProto);

        // Verify the result
        assertNotNull("MatchAllQueryBuilder should not be null", matchAllQueryBuilder);
        assertEquals("Boost should match", 3.0f, matchAllQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchAllQueryBuilder.queryName());
    }

    public void testFromProtoWithNullInput() {
        // Call the method under test with null input, should throw NullPointerException
        NullPointerException exception = expectThrows(NullPointerException.class, () -> MatchAllQueryBuilderProtoUtils.fromProto(null));
    }
}
