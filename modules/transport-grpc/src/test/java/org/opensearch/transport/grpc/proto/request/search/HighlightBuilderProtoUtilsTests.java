/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.protobufs.BoundaryScanner;
import org.opensearch.protobufs.BuiltinHighlighterType;
import org.opensearch.protobufs.Highlight;
import org.opensearch.protobufs.HighlightField;
import org.opensearch.protobufs.HighlighterEncoder;
import org.opensearch.protobufs.HighlighterFragmenter;
import org.opensearch.protobufs.HighlighterOrder;
import org.opensearch.protobufs.HighlighterTagsSchema;
import org.opensearch.protobufs.HighlighterType;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link HighlightBuilderProtoUtils}.
 */
public class HighlightBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProto_NullHighlight() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            HighlightBuilderProtoUtils.fromProto(null, registry);
        });
        assertEquals("Highlight cannot be null", exception.getMessage());
    }

    public void testFromProto_EmptyHighlight() {
        Highlight highlightProto = Highlight.newBuilder().build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        // Should create a basic HighlightBuilder with default settings
    }

    public void testFromProto_WithPreTags() {
        Highlight highlightProto = Highlight.newBuilder().addPreTags("<em>").addPreTags("<strong>").build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        String[] preTags = result.preTags();
        assertEquals(2, preTags.length);
        assertEquals("<em>", preTags[0]);
        assertEquals("<strong>", preTags[1]);
    }

    public void testFromProto_WithPostTags() {
        Highlight highlightProto = Highlight.newBuilder().addPostTags("</em>").addPostTags("</strong>").build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        String[] postTags = result.postTags();
        assertEquals(2, postTags.length);
        assertEquals("</em>", postTags[0]);
        assertEquals("</strong>", postTags[1]);
    }

    public void testFromProto_WithOrder() {
        Highlight highlightProto = Highlight.newBuilder().setOrder(HighlighterOrder.HIGHLIGHTER_ORDER_SCORE).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(HighlightBuilder.Order.SCORE, result.order());
    }

    public void testFromProto_WithHighlightFilter() {
        Highlight highlightProto = Highlight.newBuilder().setHighlightFilter(true).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertTrue(result.highlightFilter());
    }

    public void testFromProto_WithFragmentSize() {
        Highlight highlightProto = Highlight.newBuilder().setFragmentSize(100).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(100, (int) result.fragmentSize());
    }

    public void testFromProto_WithNumberOfFragments() {
        Highlight highlightProto = Highlight.newBuilder().setNumberOfFragments(5).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(5, (int) result.numOfFragments());
    }

    public void testFromProto_WithRequireFieldMatch() {
        Highlight highlightProto = Highlight.newBuilder().setRequireFieldMatch(true).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertTrue(result.requireFieldMatch());
    }

    public void testFromProto_WithBoundaryScanner() {
        Highlight highlightProto = Highlight.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_WORD).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(HighlightBuilder.BoundaryScannerType.WORD, result.boundaryScannerType());
    }

    public void testFromProto_WithBoundaryMaxScan() {
        Highlight highlightProto = Highlight.newBuilder().setBoundaryMaxScan(50).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(50, (int) result.boundaryMaxScan());
    }

    public void testFromProto_WithBoundaryChars() {
        Highlight highlightProto = Highlight.newBuilder().setBoundaryChars(".,!?").build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        char[] boundaryChars = result.boundaryChars();
        assertEquals(4, boundaryChars.length);
        assertEquals('.', boundaryChars[0]);
        assertEquals(',', boundaryChars[1]);
        assertEquals('!', boundaryChars[2]);
        assertEquals('?', boundaryChars[3]);
    }

    public void testFromProto_WithBoundaryScannerLocale() {
        Highlight highlightProto = Highlight.newBuilder().setBoundaryScannerLocale("en-US").build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertNotNull(result.boundaryScannerLocale());
        assertEquals("en-US", result.boundaryScannerLocale().toLanguageTag());
    }

    public void testFromProto_WithBuiltinHighlighterType() {
        Highlight highlightProto = Highlight.newBuilder()
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_FVH).build())
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals("fvh", result.highlighterType());
    }

    public void testFromProto_WithCustomHighlighterType() {
        Highlight highlightProto = Highlight.newBuilder()
            .setType(HighlighterType.newBuilder().setCustom("custom_highlighter").build())
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals("custom_highlighter", result.highlighterType());
    }

    public void testFromProto_WithFragmenter() {
        Highlight highlightProto = Highlight.newBuilder().setFragmenter(HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_SPAN).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals("span", result.fragmenter());
    }

    public void testFromProto_WithNoMatchSize() {
        Highlight highlightProto = Highlight.newBuilder().setNoMatchSize(200).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(200, (int) result.noMatchSize());
    }

    public void testFromProto_WithForceSource() {
        Highlight highlightProto = Highlight.newBuilder().setForceSource(true).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertTrue(result.forceSource());
    }

    public void testFromProto_WithPhraseLimit() {
        Highlight highlightProto = Highlight.newBuilder().setPhraseLimit(10).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(10, (int) result.phraseLimit());
    }

    public void testFromProto_WithMaxAnalyzedOffset() {
        Highlight highlightProto = Highlight.newBuilder().setMaxAnalyzedOffset(1000).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(1000, (int) result.maxAnalyzerOffset());
    }

    public void testFromProto_WithOptions() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("key1", "value1");
        optionsMap.put("key2", 42);

        Highlight highlightProto = Highlight.newBuilder()
            .setOptions(
                ObjectMap.newBuilder()
                    .putFields("key1", ObjectMap.Value.newBuilder().setString("value1").build())
                    .putFields("key2", ObjectMap.Value.newBuilder().setInt32(42).build())
                    .build()
            )
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        Map<String, Object> resultOptions = result.options();
        assertNotNull(resultOptions);
        assertEquals("value1", resultOptions.get("key1"));
        assertEquals(42, resultOptions.get("key2"));
    }

    public void testFromProto_WithHighlightQuery() {
        Highlight highlightProto = Highlight.newBuilder()
            .setHighlightQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertNotNull(result.highlightQuery());
        assertEquals("match_all", result.highlightQuery().getName());
        assertTrue(result.highlightQuery() instanceof org.opensearch.index.query.MatchAllQueryBuilder);

    }

    public void testFromProto_WithTagsSchema() {
        Highlight highlightProto = Highlight.newBuilder().setTagsSchema(HighlighterTagsSchema.HIGHLIGHTER_TAGS_SCHEMA_STYLED).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        // tagsSchema is a setter that applies the schema immediately, no getter available
    }

    public void testFromProto_WithEncoder() {
        Highlight highlightProto = Highlight.newBuilder().setEncoder(HighlighterEncoder.HIGHLIGHTER_ENCODER_HTML).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals("html", result.encoder());
    }

    public void testFromProto_WithFields() {
        HighlightField fieldProto = HighlightField.newBuilder()
            .setFragmentOffset(10)
            .addMatchedFields("title")
            .addMatchedFields("content")
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_PLAIN).build())
            .setBoundaryChars(".,!?")
            .setBoundaryMaxScan(30)
            .setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_SENTENCE)
            .setBoundaryScannerLocale("en-GB")
            .setFragmenter(HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_SIMPLE)
            .setFragmentSize(150)
            .setHighlightFilter(false)
            .setNoMatchSize(100)
            .setNumberOfFragments(3)
            .build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertNotNull(fields);
        assertEquals(1, fields.size());

        HighlightBuilder.Field titleField = fields.get(0);
        assertNotNull(titleField);
        assertEquals("title", titleField.name());

        assertEquals("plain", titleField.highlighterType());
        assertEquals(30, (int) titleField.boundaryMaxScan());
        assertEquals(HighlightBuilder.BoundaryScannerType.SENTENCE, titleField.boundaryScannerType());
        assertNotNull(titleField.boundaryScannerLocale());
        assertEquals("en-GB", titleField.boundaryScannerLocale().toLanguageTag());
        assertEquals("simple", titleField.fragmenter());
        assertEquals(150, (int) titleField.fragmentSize());
        assertFalse(titleField.highlightFilter());
        assertEquals(100, (int) titleField.noMatchSize());
        assertEquals(3, (int) titleField.numOfFragments());
    }

    public void testFromProto_WithFieldHighlightQuery() {
        Highlight highlightProto = Highlight.newBuilder()
            .putFields(
                "title",
                HighlightField.newBuilder()
                    .setHighlightQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
                    .build()
            )
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field titleField = fields.get(0);
        assertNotNull(titleField);
        assertNotNull(titleField.highlightQuery());

        assertEquals("match_all", titleField.highlightQuery().getName());
        assertTrue(titleField.highlightQuery() instanceof org.opensearch.index.query.MatchAllQueryBuilder);

    }

    public void testFromProto_WithComplexHighlightQuery() {
        Highlight highlightProto = Highlight.newBuilder()
            .setHighlightQuery(
                QueryContainer.newBuilder()
                    .setTerm(
                        org.opensearch.protobufs.TermQuery.newBuilder()
                            .setField("status")
                            .setValue(org.opensearch.protobufs.FieldValue.newBuilder().setString("active").build())
                            .build()
                    )
                    .build()
            )
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertNotNull(result.highlightQuery());

        assertEquals("term", result.highlightQuery().getName());
        assertTrue(result.highlightQuery() instanceof org.opensearch.index.query.TermQueryBuilder);

        org.opensearch.index.query.TermQueryBuilder termQuery = (org.opensearch.index.query.TermQueryBuilder) result.highlightQuery();
        assertEquals("status", termQuery.fieldName());
        assertEquals("active", termQuery.value());

        String queryJson = result.highlightQuery().toString();
        assertNotNull(queryJson);
        assertTrue(queryJson.contains("term"));
        assertTrue(queryJson.contains("status"));
        assertTrue(queryJson.contains("active"));
    }

    public void testFromProto_WithFieldOptions() {
        HighlightField fieldProto = HighlightField.newBuilder()
            .setOptions(
                ObjectMap.newBuilder().putFields("field_key", ObjectMap.Value.newBuilder().setString("field_value").build()).build()
            )
            .build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        HighlightBuilder.Field titleField = fields.get(0);
        assertNotNull(titleField);

        Map<String, Object> fieldOptions = titleField.options();
        assertNotNull(fieldOptions);
        assertEquals("field_value", fieldOptions.get("field_key"));
    }

    public void testFromProto_WithFieldMaxAnalyzedOffset() {
        HighlightField fieldProto = HighlightField.newBuilder().setMaxAnalyzedOffset(500).build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        HighlightBuilder.Field titleField = fields.get(0);
        assertNotNull(titleField);
        assertEquals(500, (int) titleField.maxAnalyzerOffset());
    }

    public void testFromProto_WithoutRegistry() {
        // Test error handling with null registry

        // Create a highlight with highlightQuery that requires registry
        Highlight highlightProto = Highlight.newBuilder()
            .setHighlightQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .build();

        // Should throw IllegalStateException when registry is null
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            HighlightBuilderProtoUtils.fromProto(highlightProto, null);
        });
        assertEquals("QueryBuilderProtoConverterRegistry cannot be null.", exception.getMessage());
    }

    public void testFromProto_WithUnspecifiedEnums() {
        Highlight highlightProto = Highlight.newBuilder()
            .setOrder(HighlighterOrder.HIGHLIGHTER_ORDER_UNSPECIFIED)
            .setFragmenter(HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_UNSPECIFIED)
            .setTagsSchema(HighlighterTagsSchema.HIGHLIGHTER_TAGS_SCHEMA_UNSPECIFIED)
            .setEncoder(HighlighterEncoder.HIGHLIGHTER_ENCODER_UNSPECIFIED)
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_UNSPECIFIED).build())
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertNull(result.order());
        assertNull(result.fragmenter());
        assertNull(result.encoder());
        assertNull(result.highlighterType());
    }

    public void testFromProto_WithBoundaryScannerUnspecified() {
        Highlight highlightProto = Highlight.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_UNSPECIFIED).build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertNull(result.boundaryScannerType());
    }

    public void testFromProto_ComplexScenario() {
        HighlightField titleField = HighlightField.newBuilder()
            .setFragmentSize(100)
            .setNumberOfFragments(2)
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_FVH).build())
            .build();

        HighlightField contentField = HighlightField.newBuilder()
            .setFragmentSize(200)
            .setNumberOfFragments(5)
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_PLAIN).build())
            .build();

        Highlight highlightProto = Highlight.newBuilder()
            .addPreTags("<em>")
            .addPostTags("</em>")
            .setOrder(HighlighterOrder.HIGHLIGHTER_ORDER_SCORE)
            .setFragmentSize(150)
            .setNumberOfFragments(3)
            .setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_WORD)
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_UNIFIED).build())
            .putFields("title", titleField)
            .putFields("content", contentField)
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);

        // Check global settings
        String[] preTags = result.preTags();
        assertEquals(1, preTags.length);
        assertEquals("<em>", preTags[0]);

        String[] postTags = result.postTags();
        assertEquals(1, postTags.length);
        assertEquals("</em>", postTags[0]);

        assertEquals(HighlightBuilder.Order.SCORE, result.order());
        assertEquals(150, (int) result.fragmentSize());
        assertEquals(3, (int) result.numOfFragments());
        assertEquals(HighlightBuilder.BoundaryScannerType.WORD, result.boundaryScannerType());
        assertEquals("unified", result.highlighterType());

        // Check fields
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(2, fields.size());

        HighlightBuilder.Field resultTitleField = fields.get(0);
        assertNotNull(resultTitleField);
        assertEquals("title", resultTitleField.name());
        assertEquals(100, (int) resultTitleField.fragmentSize());
        assertEquals(2, (int) resultTitleField.numOfFragments());
        assertEquals("fvh", resultTitleField.highlighterType());

        HighlightBuilder.Field resultContentField = fields.get(1);
        assertNotNull(resultContentField);
        assertEquals("content", resultContentField.name());
        assertEquals(200, (int) resultContentField.fragmentSize());
        assertEquals(5, (int) resultContentField.numOfFragments());
        assertEquals("plain", resultContentField.highlighterType());
    }
}
