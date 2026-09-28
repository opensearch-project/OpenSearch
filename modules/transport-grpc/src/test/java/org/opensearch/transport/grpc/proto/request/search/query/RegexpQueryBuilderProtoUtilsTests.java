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
import org.opensearch.index.query.RegexpFlag;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.protobufs.RegexpQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.transport.grpc.proto.request.search.query.RegexpQueryBuilderProtoUtils.fromProto;

public class RegexpQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoTestUtils.setupRegistry();
    }

    public void testFromProtoWithRequiredFieldsOnly() {
        // Create a minimal RegexpQuery proto with only required fields
        RegexpQuery proto = RegexpQuery.newBuilder().setField("test_field").setValue("test.*value").build();

        // Convert to RegexpQueryBuilder
        RegexpQueryBuilder builder = fromProto(proto);

        // Verify basic properties
        assertEquals("test_field", builder.fieldName());
        assertEquals("test.*value", builder.value());
        assertEquals(RegexpQueryBuilder.DEFAULT_FLAGS_VALUE, builder.flags());
        assertEquals(RegexpQueryBuilder.DEFAULT_CASE_INSENSITIVITY, builder.caseInsensitive());
        assertEquals(RegexpQueryBuilder.DEFAULT_DETERMINIZE_WORK_LIMIT, builder.maxDeterminizedStates());
        assertNull(builder.rewrite());
        assertEquals(1.0f, builder.boost(), 0.001f);
        assertNull(builder.queryName());
    }

    public void testFromProtoWithAllFields() {
        // Create a complete RegexpQuery proto with all fields set
        RegexpQuery proto = RegexpQuery.newBuilder()
            .setField("test_field")
            .setValue("test.*value")
            .setBoost(2.0f)
            .setXName("test_query")
            .setFlags("INTERSECTION|COMPLEMENT")
            .setCaseInsensitive(true)
            .setMaxDeterminizedStates(20000)
            .setRewrite("constant_score")
            .build();

        // Convert to RegexpQueryBuilder
        RegexpQueryBuilder builder = fromProto(proto);

        // Verify all properties
        assertEquals("test_field", builder.fieldName());
        assertEquals("test.*value", builder.value());
        assertEquals(RegexpFlag.INTERSECTION.value() | RegexpFlag.COMPLEMENT.value(), builder.flags());
        assertTrue(builder.caseInsensitive());
        assertEquals(20000, builder.maxDeterminizedStates());
        assertEquals("constant_score", builder.rewrite());
        assertEquals(2.0f, builder.boost(), 0.001f);
        assertEquals("test_query", builder.queryName());
    }

    public void testFromProtoWithDifferentRewriteMethods() {
        // Test all possible rewrite methods
        String[] rewriteMethods = {
            "constant_score",
            "constant_score_boolean",
            "scoring_boolean",
            "top_terms_10",
            "top_terms_blended_freqs_10",
            "top_terms_boost_10" };

        String[] expectedRewriteMethods = {
            "constant_score",
            "constant_score_boolean",
            "scoring_boolean",
            "top_terms_10",
            "top_terms_blended_freqs_10",
            "top_terms_boost_10" };

        for (int i = 0; i < rewriteMethods.length; i++) {
            RegexpQuery proto = RegexpQuery.newBuilder()
                .setField("test_field")
                .setValue("test.*value")
                .setRewrite(rewriteMethods[i])
                .build();

            RegexpQueryBuilder builder = fromProto(proto);
            assertEquals(expectedRewriteMethods[i], builder.rewrite());
        }
    }

    /**
     * Test that compares the results of fromXContent and fromProto to ensure they produce equivalent results.
     */
    public void testFromProtoMatchesFromXContent() throws IOException {
        // 1. Create a JSON string for XContent parsing
        String json = "{\n"
            + "    \"test_field\": {\n"
            + "      \"value\": \"test.*value\",\n"
            + "      \"flags\": \"INTERSECTION\",\n"
            + "      \"case_insensitive\": true,\n"
            + "      \"max_determinized_states\": 20000,\n"
            + "      \"rewrite\": \"constant_score\",\n"
            + "      \"boost\": 2.0,\n"
            + "      \"_name\": \"test_query\"\n"
            + "    }\n"
            + "}";

        // 2. Parse the JSON to create a RegexpQueryBuilder via fromXContent
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // Move to the first token
        RegexpQueryBuilder fromXContent = RegexpQueryBuilder.fromXContent(parser);

        // 3. Create an equivalent RegexpQuery proto
        RegexpQuery proto = RegexpQuery.newBuilder()
            .setField("test_field")
            .setValue("test.*value")
            .setFlags("INTERSECTION")
            .setCaseInsensitive(true)
            .setMaxDeterminizedStates(20000)
            .setRewrite("constant_score")
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        // 4. Convert the proto to a RegexpQueryBuilder
        RegexpQueryBuilder fromProto = RegexpQueryBuilderProtoUtils.fromProto(proto);

        // 5. Compare the two builders
        assertEquals(fromXContent.fieldName(), fromProto.fieldName());
        assertEquals(fromXContent.value(), fromProto.value());
        assertEquals(fromXContent.flags(), fromProto.flags());
        assertEquals(fromXContent.caseInsensitive(), fromProto.caseInsensitive());
        assertEquals(fromXContent.maxDeterminizedStates(), fromProto.maxDeterminizedStates());
        assertEquals(fromXContent.rewrite(), fromProto.rewrite());
        assertEquals(fromXContent.boost(), fromProto.boost(), 0.001f);
        assertEquals(fromXContent.queryName(), fromProto.queryName());
    }
}
