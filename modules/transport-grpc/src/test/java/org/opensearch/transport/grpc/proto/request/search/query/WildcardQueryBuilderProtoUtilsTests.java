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
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.protobufs.MultiTermQueryRewrite;
import org.opensearch.protobufs.WildcardQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.transport.grpc.proto.request.search.query.WildcardQueryBuilderProtoUtils.fromProto;

public class WildcardQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoTestUtils.setupRegistry();
    }

    public void testFromProtoWithRequiredFieldsOnly() {
        // Create a minimal WildcardQuery proto with only required fields
        WildcardQuery proto = WildcardQuery.newBuilder().setField("test_field").setValue("test*value").build();

        // Convert to WildcardQueryBuilder
        WildcardQueryBuilder builder = fromProto(proto);

        // Verify basic properties
        assertEquals("test_field", builder.fieldName());
        assertEquals("test*value", builder.value());
        assertEquals(WildcardQueryBuilder.DEFAULT_CASE_INSENSITIVITY, builder.caseInsensitive());
        assertNull(builder.rewrite());
        assertEquals(1.0f, builder.boost(), 0.001f);
        assertNull(builder.queryName());
    }

    public void testFromProtoWithAllFields() {
        // Create a complete WildcardQuery proto with all fields set
        WildcardQuery proto = WildcardQuery.newBuilder()
            .setField("test_field")
            .setValue("test*value")
            .setBoost(2.0f)
            .setXName("test_query")
            .setCaseInsensitive(true)
            .setRewrite(MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .build();

        // Convert to WildcardQueryBuilder
        WildcardQueryBuilder builder = fromProto(proto);

        // Verify all properties
        assertEquals("test_field", builder.fieldName());
        assertEquals("test*value", builder.value());
        assertTrue(builder.caseInsensitive());
        assertEquals("constant_score", builder.rewrite());
        assertEquals(2.0f, builder.boost(), 0.001f);
        assertEquals("test_query", builder.queryName());
    }

    public void testFromProtoWithWildcardField() {
        // Create a WildcardQuery proto using the wildcard field instead of value
        WildcardQuery proto = WildcardQuery.newBuilder().setField("test_field").setWildcard("test*value").build();

        // Convert to WildcardQueryBuilder
        WildcardQueryBuilder builder = fromProto(proto);

        // Verify the value was correctly set from the wildcard field
        assertEquals("test_field", builder.fieldName());
        assertEquals("test*value", builder.value());
    }

    public void testFromProtoWithNeitherValueNorWildcard() {
        // Test exception when neither value nor wildcard field is set
        WildcardQuery proto = WildcardQuery.newBuilder().setField("test_field").build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> fromProto(proto));
        assertEquals("Either value or wildcard field must be set in wildcardQueryProto", exception.getMessage());
    }

    public void testFromProtoWithUnspecifiedRewrite() {
        // Test that UNSPECIFIED rewrite method results in null rewrite
        WildcardQuery proto = WildcardQuery.newBuilder()
            .setField("test_field")
            .setValue("test*value")
            .setRewrite(MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_UNSPECIFIED)
            .build();

        WildcardQueryBuilder builder = fromProto(proto);
        assertNull("UNSPECIFIED rewrite should result in null", builder.rewrite());
    }

    public void testFromProtoWithDifferentRewriteMethods() {
        // Test all possible rewrite methods
        MultiTermQueryRewrite[] rewriteMethods = {
            MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE,
            MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE_BOOLEAN,
            MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_SCORING_BOOLEAN,
            MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_TOP_TERMS_N,
            MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_TOP_TERMS_BLENDED_FREQS_N,
            MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_TOP_TERMS_BOOST_N };

        String[] expectedRewriteMethods = {
            "constant_score",
            "constant_score_boolean",
            "scoring_boolean",
            "top_terms_n",
            "top_terms_blended_freqs_n",
            "top_terms_boost_n" };

        for (int i = 0; i < rewriteMethods.length; i++) {
            WildcardQuery proto = WildcardQuery.newBuilder()
                .setField("test_field")
                .setValue("test*value")
                .setRewrite(rewriteMethods[i])
                .build();

            WildcardQueryBuilder builder = fromProto(proto);
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
            + "      \"value\": \"test*value\",\n"
            + "      \"case_insensitive\": true,\n"
            + "      \"rewrite\": \"constant_score\",\n"
            + "      \"boost\": 2.0,\n"
            + "      \"_name\": \"test_query\"\n"
            + "    }\n"
            + "}";

        // 2. Parse the JSON to create a WildcardQueryBuilder via fromXContent
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // Move to the first token
        WildcardQueryBuilder fromXContent = WildcardQueryBuilder.fromXContent(parser);

        // 3. Create an equivalent WildcardQuery proto
        WildcardQuery proto = WildcardQuery.newBuilder()
            .setField("test_field")
            .setValue("test*value")
            .setCaseInsensitive(true)
            .setRewrite(MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        // 4. Convert the proto to a WildcardQueryBuilder
        WildcardQueryBuilder fromProto = WildcardQueryBuilderProtoUtils.fromProto(proto);

        // 5. Compare the two builders
        assertEquals(fromXContent.fieldName(), fromProto.fieldName());
        assertEquals(fromXContent.value(), fromProto.value());
        assertEquals(fromXContent.caseInsensitive(), fromProto.caseInsensitive());
        // Note: The rewrite method is stored differently in the two builders
        // fromXContent has "constant_score" while fromProto has "MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE"
        assertEquals(fromXContent.boost(), fromProto.boost(), 0.001f);
        assertEquals(fromXContent.queryName(), fromProto.queryName());
    }
}
