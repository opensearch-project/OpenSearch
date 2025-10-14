/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.TermsSetQueryBuilder;
import org.opensearch.protobufs.BuiltinScriptLanguage;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.TermsSetQuery;
import org.opensearch.test.OpenSearchTestCase;

public class TermsSetQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoBasic() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("status")
            .addTerms("published")
            .setMinimumShouldMatchField("count")
            .build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals(1, result.getValues().size());
        Object value = result.getValues().get(0);
        String stringValue = value instanceof org.apache.lucene.util.BytesRef
            ? ((org.apache.lucene.util.BytesRef) value).utf8ToString()
            : value.toString();
        assertEquals("published", stringValue);
        assertEquals(AbstractQueryBuilder.DEFAULT_BOOST, result.boost(), 0.0f);
        assertNull(result.queryName());
    }

    public void testFromProtoWithOptionalFields() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("tags")
            .addTerms("urgent")
            .setBoost(2.0f)
            .setXName("test_query")
            .setMinimumShouldMatchField("tag_count")
            .build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals(2.0f, result.boost(), 0.0f);
        assertEquals("test_query", result.queryName());
        assertEquals("tag_count", result.getMinimumShouldMatchField());
    }

    public void testFromProtoWithScript() {
        Script script = Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("params.num_terms")
                    .setLang(ScriptLanguage.newBuilder().setBuiltin(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                    .build()
            )
            .build();

        TermsSetQuery proto = TermsSetQuery.newBuilder().setField("skills").addTerms("java").setMinimumShouldMatchScript(script).build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertNull(result.getMinimumShouldMatchField());
        assertNotNull(result.getMinimumShouldMatchScript());
        assertEquals("painless", result.getMinimumShouldMatchScript().getLang());
    }

    public void testFromProtoWithNullInput() {
        expectThrows(IllegalArgumentException.class, () -> TermsSetQueryBuilderProtoUtils.fromProto(null));
    }

    public void testFromProtoWithEmptyField() {
        TermsSetQuery proto = TermsSetQuery.newBuilder().setField("").addTerms("value").setMinimumShouldMatchField("count").build();

        expectThrows(IllegalArgumentException.class, () -> TermsSetQueryBuilderProtoUtils.fromProto(proto));
    }

    public void testFromProtoWithNoTerms() {
        TermsSetQuery proto = TermsSetQuery.newBuilder().setField("category").setMinimumShouldMatchField("count").build();

        expectThrows(IllegalArgumentException.class, () -> TermsSetQueryBuilderProtoUtils.fromProto(proto));
    }

    public void testFromProtoWithNullField() {
        // No field set should default to null or empty
        TermsSetQuery proto = TermsSetQuery.newBuilder().addTerms("value").setMinimumShouldMatchField("count").build();

        expectThrows(IllegalArgumentException.class, () -> TermsSetQueryBuilderProtoUtils.fromProto(proto));
    }

    public void testFromProtoWithMultipleTerms() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("categories")
            .addTerms("tech")
            .addTerms("science")
            .addTerms("programming")
            .addTerms("java")
            .setMinimumShouldMatchField("category_count")
            .setBoost(1.5f)
            .setXName("multi_terms_query")
            .build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals(4, result.getValues().size());
        // Note: Field name is internal to the builder and not directly accessible for verification
        assertEquals("category_count", result.getMinimumShouldMatchField());
        assertEquals(1.5f, result.boost(), 0.0f);
        assertEquals("multi_terms_query", result.queryName());
    }

    public void testFromProtoMinimalFieldsOnly() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("minimal_field")
            .addTerms("single_term")
            .setMinimumShouldMatchField("min_count")
            .build(); // No boost or query name - should use defaults

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals(1, result.getValues().size());
        assertEquals("min_count", result.getMinimumShouldMatchField());
        assertEquals(AbstractQueryBuilder.DEFAULT_BOOST, result.boost(), 0.0f);
        assertNull(result.queryName());
    }

    public void testFromProtoWithEmptyTermsString() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("test_field")
            .addTerms("") // Empty string term
            .addTerms("valid_term")
            .setMinimumShouldMatchField("count")
            .build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals(2, result.getValues().size());
        // Both empty string and valid term should be included
    }

    public void testFromProtoWithDuplicateTerms() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("duplicate_field")
            .addTerms("term1")
            .addTerms("term2")
            .addTerms("term1") // Duplicate
            .addTerms("term2") // Duplicate
            .setMinimumShouldMatchField("term_count")
            .build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals(4, result.getValues().size()); // All terms included, including duplicates
    }

    public void testFromProtoWithZeroBoost() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("boost_field")
            .addTerms("term")
            .setBoost(0.0f) // Zero boost
            .setMinimumShouldMatchField("count")
            .build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals(0.0f, result.boost(), 0.0f);
    }

    public void testFromProtoWithNegativeBoost() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("negative_boost_field")
            .addTerms("term")
            .setBoost(-1.0f) // Negative boost - should throw exception
            .setMinimumShouldMatchField("count")
            .build();

        expectThrows(IllegalArgumentException.class, () -> TermsSetQueryBuilderProtoUtils.fromProto(proto));
    }

    public void testFromProtoWithBothFieldAndScript() {
        Script script = Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("params.required_count")
                    .setLang(ScriptLanguage.newBuilder().setBuiltin(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                    .build()
            )
            .build();

        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("field_with_script")
            .addTerms("term1")
            .setMinimumShouldMatchField("field_count") // Both field and script
            .setMinimumShouldMatchScript(script)
            .build();

        // OpenSearch TermsSetQueryBuilder doesn't allow both field and script to be set
        // This should throw an IllegalArgumentException
        expectThrows(IllegalArgumentException.class, () -> TermsSetQueryBuilderProtoUtils.fromProto(proto));
    }

    public void testFromProtoWithEmptyQueryName() {
        TermsSetQuery proto = TermsSetQuery.newBuilder()
            .setField("query_name_field")
            .addTerms("term")
            .setXName("") // Empty query name
            .setMinimumShouldMatchField("count")
            .build();

        TermsSetQueryBuilder result = TermsSetQueryBuilderProtoUtils.fromProto(proto);

        assertNotNull(result);
        assertEquals("", result.queryName()); // Empty string should be preserved
    }

}
