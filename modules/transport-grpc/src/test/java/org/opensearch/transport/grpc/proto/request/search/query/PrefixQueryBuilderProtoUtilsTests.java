/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.protobufs.MultiTermQueryRewrite;
import org.opensearch.protobufs.PrefixQuery;
import org.opensearch.test.OpenSearchTestCase;

public class PrefixQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithBasicPrefixQuery() {
        PrefixQuery prefixQuery = PrefixQuery.newBuilder().setField("name").setValue("joh").build();

        PrefixQueryBuilder prefixQueryBuilder = PrefixQueryBuilderProtoUtils.fromProto(prefixQuery);

        assertNotNull("PrefixQueryBuilder should not be null", prefixQueryBuilder);
        assertEquals("Field name should match", "name", prefixQueryBuilder.fieldName());
        assertEquals("Value should match", "joh", prefixQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, prefixQueryBuilder.boost(), 0.0f);
        assertEquals(
            "Default case insensitive should match",
            PrefixQueryBuilder.DEFAULT_CASE_INSENSITIVITY,
            prefixQueryBuilder.caseInsensitive()
        );
    }

    public void testFromProtoWithAllParameters() {
        PrefixQuery prefixQuery = PrefixQuery.newBuilder()
            .setField("name")
            .setValue("joh")
            .setRewrite(MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .setCaseInsensitive(true)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        PrefixQueryBuilder prefixQueryBuilder = PrefixQueryBuilderProtoUtils.fromProto(prefixQuery);

        assertNotNull("PrefixQueryBuilder should not be null", prefixQueryBuilder);
        assertEquals("Field name should match", "name", prefixQueryBuilder.fieldName());
        assertEquals("Value should match", "joh", prefixQueryBuilder.value());
        assertEquals("Rewrite should match", "constant_score", prefixQueryBuilder.rewrite());
        assertEquals("Case insensitive should match", true, prefixQueryBuilder.caseInsensitive());
        assertEquals("Boost should match", 2.0f, prefixQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_query", prefixQueryBuilder.queryName());
    }

    public void testFromProtoWithCaseInsensitiveFalse() {
        PrefixQuery prefixQuery = PrefixQuery.newBuilder().setField("name").setValue("joh").setCaseInsensitive(false).build();

        PrefixQueryBuilder prefixQueryBuilder = PrefixQueryBuilderProtoUtils.fromProto(prefixQuery);

        assertNotNull("PrefixQueryBuilder should not be null", prefixQueryBuilder);
        assertEquals("Case insensitive should match", false, prefixQueryBuilder.caseInsensitive());
    }

    public void testFromProtoWithMinimalFields() {
        PrefixQuery prefixQuery = PrefixQuery.newBuilder().setField("title").setValue("ope").build();

        PrefixQueryBuilder prefixQueryBuilder = PrefixQueryBuilderProtoUtils.fromProto(prefixQuery);

        assertNotNull("PrefixQueryBuilder should not be null", prefixQueryBuilder);
        assertEquals("Field name should match", "title", prefixQueryBuilder.fieldName());
        assertEquals("Value should match", "ope", prefixQueryBuilder.value());
        assertNull("Query name should be null", prefixQueryBuilder.queryName());
    }
}
