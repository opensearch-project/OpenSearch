/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.protobufs.ExistsQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.transport.grpc.proto.request.search.query.ExistsQueryBuilderProtoUtils.fromProto;

public class ExistsQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoTestUtils.setupRegistry();
    }

    public void testFromProtoWithRequiredFieldsOnly() {
        // Create a minimal ExistsQuery proto with only required fields
        ExistsQuery proto = ExistsQuery.newBuilder().setField("test_field").build();

        // Convert to ExistsQueryBuilder
        ExistsQueryBuilder builder = fromProto(proto);

        // Verify basic properties
        assertEquals("test_field", builder.fieldName());
        assertEquals(1.0f, builder.boost(), 0.001f);
        assertNull(builder.queryName());
    }

    public void testFromProtoWithAllFields() {
        // Create a complete ExistsQuery proto with all fields set
        ExistsQuery proto = ExistsQuery.newBuilder().setField("test_field").setBoost(2.0f).setXName("test_query").build();

        // Convert to ExistsQueryBuilder
        ExistsQueryBuilder builder = fromProto(proto);

        // Verify all properties
        assertEquals("test_field", builder.fieldName());
        assertEquals(2.0f, builder.boost(), 0.001f);
        assertEquals("test_query", builder.queryName());
    }

    public void testFromProtoWithNullInput() {
        // Test null input should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> fromProto(null));
        assertEquals("ExistsQuery cannot be null", exception.getMessage());
    }

    /**
     * Test that compares the results of fromXContent and fromProto to ensure they produce equivalent results.
     */
    public void testFromProtoMatchesFromXContent() throws IOException {
        // 1. Create a JSON string for XContent parsing
        String json = "{\n" + "  \"field\": \"test_field\",\n" + "  \"boost\": 2.0,\n" + "  \"_name\": \"test_query\"\n" + "}";

        // 2. Parse the JSON to create an ExistsQueryBuilder via fromXContent
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // Move to the first token
        ExistsQueryBuilder fromXContent = ExistsQueryBuilder.fromXContent(parser);

        // 3. Create an equivalent ExistsQuery proto
        ExistsQuery proto = ExistsQuery.newBuilder().setField("test_field").setBoost(2.0f).setXName("test_query").build();

        // 4. Convert the proto to an ExistsQueryBuilder
        ExistsQueryBuilder fromProto = ExistsQueryBuilderProtoUtils.fromProto(proto);

        // 5. Compare the two builders
        assertEquals(fromXContent.fieldName(), fromProto.fieldName());
        assertEquals(fromXContent.boost(), fromProto.boost(), 0.001f);
        assertEquals(fromXContent.queryName(), fromProto.queryName());
    }
}
