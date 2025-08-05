/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.protobufs.FieldCollapse;
import org.opensearch.protobufs.InnerHits;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CollapseBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithBasicField() throws IOException {
        // Create a protobuf FieldCollapse with just a field name
        FieldCollapse fieldCollapse = FieldCollapse.newBuilder().setField("user_id").build();

        // Call the method under test
        CollapseBuilder collapseBuilder = CollapseBuilderProtoUtils.fromProto(fieldCollapse);

        // Verify the result
        assertNotNull("CollapseBuilder should not be null", collapseBuilder);
        assertEquals("Field name should match", "user_id", collapseBuilder.getField());
        assertEquals("MaxConcurrentGroupRequests should be default", 0, collapseBuilder.getMaxConcurrentGroupRequests());
        assertEquals("InnerHits should be empty", 0, collapseBuilder.getInnerHits().size());
    }

    public void testFromProtoWithMaxConcurrentGroupSearches() throws IOException {
        // Create a protobuf FieldCollapse with maxConcurrentGroupSearches
        FieldCollapse fieldCollapse = FieldCollapse.newBuilder().setField("user_id").setMaxConcurrentGroupSearches(10).build();

        // Call the method under test
        CollapseBuilder collapseBuilder = CollapseBuilderProtoUtils.fromProto(fieldCollapse);

        // Verify the result
        assertNotNull("CollapseBuilder should not be null", collapseBuilder);
        assertEquals("Field name should match", "user_id", collapseBuilder.getField());
        assertEquals("MaxConcurrentGroupRequests should match", 10, collapseBuilder.getMaxConcurrentGroupRequests());
        assertEquals("InnerHits should be empty", 0, collapseBuilder.getInnerHits().size());
    }

    public void testFromProtoWithInnerHits() throws IOException {
        // Create a protobuf FieldCollapse with inner hits
        FieldCollapse fieldCollapse = FieldCollapse.newBuilder()
            .setField("user_id")
            .addInnerHits(InnerHits.newBuilder().setName("last_tweet").setSize(5).build())
            .build();

        // Call the method under test
        CollapseBuilder collapseBuilder = CollapseBuilderProtoUtils.fromProto(fieldCollapse);

        // Verify the result
        assertNotNull("CollapseBuilder should not be null", collapseBuilder);
        assertEquals("Field name should match", "user_id", collapseBuilder.getField());
        assertNotNull("InnerHits should not be null", collapseBuilder.getInnerHits());
        assertEquals("InnerHits name should match", "last_tweet", collapseBuilder.getInnerHits().get(0).getName());
        assertEquals("InnerHits size should match", 5, collapseBuilder.getInnerHits().get(0).getSize());
    }

    public void testFromProtoWithMultipleInnerHits() throws IOException {
        // Create a protobuf FieldCollapse with multiple inner hits
        FieldCollapse fieldCollapse = FieldCollapse.newBuilder()
            .setField("user_id")
            .addInnerHits(InnerHits.newBuilder().setName("first_inner_hit").setSize(5).build())
            .addInnerHits(InnerHits.newBuilder().setName("second_inner_hit").setSize(10).build())
            .build();

        // Call the method under test
        CollapseBuilder collapseBuilder = CollapseBuilderProtoUtils.fromProto(fieldCollapse);

        // Verify the result
        assertNotNull("CollapseBuilder should not be null", collapseBuilder);
        assertEquals("Field name should match", "user_id", collapseBuilder.getField());
        assertNotNull("InnerHits should not be null", collapseBuilder.getInnerHits());
        // The last inner hit in the list should be used
        assertEquals("InnerHits name should match the last inner hit", "second_inner_hit", collapseBuilder.getInnerHits().get(0).getName());
        assertEquals("InnerHits size should match the last inner hit", 10, collapseBuilder.getInnerHits().get(0).getSize());
    }

    public void testFromProtoWithAllFields() throws IOException {
        // Create a protobuf FieldCollapse with all fields
        FieldCollapse fieldCollapse = FieldCollapse.newBuilder()
            .setField("user_id")
            .setMaxConcurrentGroupSearches(10)
            .addInnerHits(InnerHits.newBuilder().setName("last_tweet").setSize(5).build())
            .build();

        // Call the method under test
        CollapseBuilder collapseBuilder = CollapseBuilderProtoUtils.fromProto(fieldCollapse);

        // Verify the result
        assertNotNull("CollapseBuilder should not be null", collapseBuilder);
        assertEquals("Field name should match", "user_id", collapseBuilder.getField());
        assertEquals("MaxConcurrentGroupRequests should match", 10, collapseBuilder.getMaxConcurrentGroupRequests());
        assertNotNull("InnerHits should not be null", collapseBuilder.getInnerHits());
        assertEquals("InnerHits name should match", "last_tweet", collapseBuilder.getInnerHits().get(0).getName());
        assertEquals("InnerHits size should match", 5, collapseBuilder.getInnerHits().get(0).getSize());
    }

}
