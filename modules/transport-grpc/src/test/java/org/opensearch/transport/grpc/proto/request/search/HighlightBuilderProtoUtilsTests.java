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

    public void testFromProto_WithoutRegistry() {
        Highlight highlightWithQuery = Highlight.newBuilder()
            .setHighlightQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .build();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            HighlightBuilderProtoUtils.fromProto(highlightWithQuery, null);
        });

        assertEquals("QueryBuilderProtoConverterRegistry cannot be null.", exception.getMessage());
    }

    public void testFromProto_EmptyHighlight() {
        Highlight highlightProto = Highlight.newBuilder().build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);
        assertNotNull(result);
    }

    public void testFromProto_WithTagsAndBasicSettings() {
        Highlight highlightProto = Highlight.newBuilder()
            .addPreTags("<em>")
            .addPreTags("<strong>")
            .addPostTags("</em>")
            .addPostTags("</strong>")
            .setOrder(HighlighterOrder.HIGHLIGHTER_ORDER_SCORE)
            .setFragmentSize(100)
            .setNumberOfFragments(5)
            .setRequireFieldMatch(true)
            .setHighlightFilter(true)
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        String[] preTags = result.preTags();
        assertEquals(2, preTags.length);
        assertEquals("<em>", preTags[0]);
        assertEquals("<strong>", preTags[1]);

        String[] postTags = result.postTags();
        assertEquals(2, postTags.length);
        assertEquals("</em>", postTags[0]);
        assertEquals("</strong>", postTags[1]);

        assertEquals(HighlightBuilder.Order.SCORE, result.order());
        assertEquals(100, (int) result.fragmentSize());
        assertEquals(5, (int) result.numOfFragments());
        assertTrue(result.requireFieldMatch());
        assertTrue(result.highlightFilter());
    }

    public void testFromProto_WithBoundarySettings() {
        Highlight highlightProto = Highlight.newBuilder()
            .setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_WORD)
            .setBoundaryMaxScan(50)
            .setBoundaryChars(".,!?")
            .setBoundaryScannerLocale("en-US")
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals(HighlightBuilder.BoundaryScannerType.WORD, result.boundaryScannerType());
        assertEquals(50, (int) result.boundaryMaxScan());
        char[] boundaryChars = result.boundaryChars();
        assertEquals(4, boundaryChars.length);
        assertEquals('.', boundaryChars[0]);
        assertNotNull(result.boundaryScannerLocale());
        assertEquals("en-US", result.boundaryScannerLocale().toLanguageTag());
    }

    public void testFromProto_WithHighlighterSettings() {
        Highlight highlightProto = Highlight.newBuilder()
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_FVH).build())
            .setFragmenter(HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_SPAN)
            .setEncoder(HighlighterEncoder.HIGHLIGHTER_ENCODER_HTML)
            .setNoMatchSize(200)
            .setForceSource(true)
            .setPhraseLimit(10)
            .setMaxAnalyzerOffset(1000)
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertEquals("fvh", result.highlighterType());
        assertEquals("span", result.fragmenter());
        assertEquals("html", result.encoder());
        assertEquals(200, (int) result.noMatchSize());
        assertTrue(result.forceSource());
        assertEquals(10, (int) result.phraseLimit());
        assertEquals(1000, (int) result.maxAnalyzerOffset());
    }

    public void testFromProto_WithOptions() {
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
    }

    public void testFromProto_WithUnspecifiedEnums() {
        Highlight highlightProto = Highlight.newBuilder()
            .setOrder(HighlighterOrder.HIGHLIGHTER_ORDER_UNSPECIFIED)
            .setFragmenter(HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_UNSPECIFIED)
            .setEncoder(HighlighterEncoder.HIGHLIGHTER_ENCODER_UNSPECIFIED)
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_UNSPECIFIED).build())
            .setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_UNSPECIFIED)
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        assertNull(result.order());
        assertNull(result.fragmenter());
        assertNull(result.encoder());
        assertNull(result.highlighterType());
        assertNull(result.boundaryScannerType());
    }

    public void testFromProto_WithFieldPreAndPostTags() {
        HighlightField fieldProto = HighlightField.newBuilder()
            .addPreTags("<em>")
            .addPreTags("<strong>")
            .addPostTags("</em>")
            .addPostTags("</strong>")
            .build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field titleField = fields.get(0);
        assertEquals("title", titleField.name());

        String[] preTags = titleField.preTags();
        assertNotNull(preTags);
        assertEquals(2, preTags.length);
        assertEquals("<em>", preTags[0]);
        assertEquals("<strong>", preTags[1]);

        String[] postTags = titleField.postTags();
        assertNotNull(postTags);
        assertEquals(2, postTags.length);
        assertEquals("</em>", postTags[0]);
        assertEquals("</strong>", postTags[1]);
    }

    public void testFromProto_WithFieldOptions() {
        HighlightField fieldProto = HighlightField.newBuilder()
            .setOptions(
                ObjectMap.newBuilder().putFields("field_key", ObjectMap.Value.newBuilder().setString("field_value").build()).build()
            )
            .setMaxAnalyzerOffset(500)
            .setHighlightQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        HighlightBuilder.Field titleField = fields.get(0);

        Map<String, Object> fieldOptions = titleField.options();
        assertNotNull(fieldOptions);
        assertEquals("field_value", fieldOptions.get("field_key"));

        assertEquals(500, (int) titleField.maxAnalyzerOffset());
        assertNotNull(titleField.highlightQuery());
        assertEquals("match_all", titleField.highlightQuery().getName());
    }

    public void testFromProto_WithMultipleFields() {
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

    public void testFromProto_WithAllBoundaryScannerTypes() {
        Highlight highlightChars = Highlight.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_CHARS).build();
        HighlightBuilder resultChars = HighlightBuilderProtoUtils.fromProto(highlightChars, registry);
        assertEquals(HighlightBuilder.BoundaryScannerType.CHARS, resultChars.boundaryScannerType());

        Highlight highlightWord = Highlight.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_WORD).build();
        HighlightBuilder resultWord = HighlightBuilderProtoUtils.fromProto(highlightWord, registry);
        assertEquals(HighlightBuilder.BoundaryScannerType.WORD, resultWord.boundaryScannerType());

        Highlight highlightSentence = Highlight.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_SENTENCE).build();
        HighlightBuilder resultSentence = HighlightBuilderProtoUtils.fromProto(highlightSentence, registry);
        assertEquals(HighlightBuilder.BoundaryScannerType.SENTENCE, resultSentence.boundaryScannerType());
    }

    public void testFromProto_WithFieldBoundaryScannerTypes() {
        HighlightField fieldChars = HighlightField.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_CHARS).build();

        HighlightField fieldWord = HighlightField.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_WORD).build();

        HighlightField fieldSentence = HighlightField.newBuilder().setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_SENTENCE).build();

        Highlight highlightProto = Highlight.newBuilder()
            .putFields("field1", fieldChars)
            .putFields("field2", fieldWord)
            .putFields("field3", fieldSentence)
            .build();

        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(3, fields.size());
        assertEquals(HighlightBuilder.BoundaryScannerType.CHARS, fields.get(0).boundaryScannerType());
        assertEquals(HighlightBuilder.BoundaryScannerType.WORD, fields.get(1).boundaryScannerType());
        assertEquals(HighlightBuilder.BoundaryScannerType.SENTENCE, fields.get(2).boundaryScannerType());
    }

    public void testFromProto_WithFieldFragmenter() {
        HighlightField fieldProto = HighlightField.newBuilder().setFragmenter(HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_SIMPLE).build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());
        assertEquals("simple", fields.get(0).fragmenter());
    }

    public void testFromProto_WithFieldBoundaryCharsAndLocale() {
        HighlightField fieldProto = HighlightField.newBuilder().setBoundaryChars(".,!?;:").setBoundaryScannerLocale("fr-FR").build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field field = fields.get(0);
        char[] boundaryChars = field.boundaryChars();
        assertEquals(6, boundaryChars.length);
        assertNotNull(field.boundaryScannerLocale());
        assertEquals("fr-FR", field.boundaryScannerLocale().toLanguageTag());
    }

    public void testFromProto_WithFieldHighlightQueryAndNoMatchSize() {
        HighlightField fieldProto = HighlightField.newBuilder()
            .setHighlightQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .setNoMatchSize(250)
            .build();

        Highlight highlightProto = Highlight.newBuilder().putFields("content", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field field = fields.get(0);
        assertNotNull(field.highlightQuery());
        assertEquals("match_all", field.highlightQuery().getName());
        assertEquals(250, (int) field.noMatchSize());
    }

    public void testFromProto_WithFieldOrder() {
        HighlightField fieldProto = HighlightField.newBuilder().setOrder(HighlighterOrder.HIGHLIGHTER_ORDER_SCORE).build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field field = fields.get(0);
        assertEquals(HighlightBuilder.Order.SCORE, field.order());
    }

    public void testFromProto_WithFieldPhraseLimit() {
        HighlightField fieldProto = HighlightField.newBuilder().setPhraseLimit(512).build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field field = fields.get(0);
        assertEquals(512, (int) field.phraseLimit());
    }

    public void testFromProto_WithFieldRequireFieldMatch() {
        HighlightField fieldProto = HighlightField.newBuilder().setRequireFieldMatch(false).build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field field = fields.get(0);
        assertFalse(field.requireFieldMatch());
    }

    public void testFromProto_WithFieldTagsSchema() {
        HighlightField fieldProto = HighlightField.newBuilder().setTagsSchema(HighlighterTagsSchema.HIGHLIGHTER_TAGS_SCHEMA_STYLED).build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        // The tags schema should have been applied - verify the field was created
        HighlightBuilder.Field field = fields.get(0);
        assertEquals("title", field.name());
    }

    public void testFromProto_WithFieldForceSource() {
        HighlightField fieldProto = HighlightField.newBuilder().setForceSource(true).build();

        Highlight highlightProto = Highlight.newBuilder().putFields("content", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field field = fields.get(0);
        assertTrue(field.forceSource());
    }

    public void testFromProto_WithAllFieldSettings() {
        // Test with all field settings combined (excluding tagsSchema to test explicit pre/post tags)
        HighlightField fieldProto = HighlightField.newBuilder()
            .setFragmentSize(150)
            .setNumberOfFragments(3)
            .setFragmentOffset(10)
            .setOrder(HighlighterOrder.HIGHLIGHTER_ORDER_SCORE)
            .setPhraseLimit(256)
            .setRequireFieldMatch(true)
            .setForceSource(true)
            .setNoMatchSize(100)
            .setType(HighlighterType.newBuilder().setBuiltin(BuiltinHighlighterType.BUILTIN_HIGHLIGHTER_TYPE_FVH).build())
            .setBoundaryScanner(BoundaryScanner.BOUNDARY_SCANNER_WORD)
            .setBoundaryMaxScan(30)
            .setBoundaryChars(".,!?")
            .setBoundaryScannerLocale("en-US")
            .setFragmenter(HighlighterFragmenter.HIGHLIGHTER_FRAGMENTER_SPAN)
            .setHighlightFilter(true)
            .setMaxAnalyzerOffset(1000)
            .addPreTags("<mark>")
            .addPostTags("</mark>")
            .addMatchedFields("title")
            .addMatchedFields("title.raw")
            .build();

        Highlight highlightProto = Highlight.newBuilder().putFields("title", fieldProto).build();
        HighlightBuilder result = HighlightBuilderProtoUtils.fromProto(highlightProto, registry);

        assertNotNull(result);
        List<HighlightBuilder.Field> fields = result.fields();
        assertEquals(1, fields.size());

        HighlightBuilder.Field field = fields.get(0);
        assertEquals("title", field.name());
        assertEquals(150, (int) field.fragmentSize());
        assertEquals(3, (int) field.numOfFragments());
        // fragmentOffset is package-private, can't verify directly
        assertEquals(HighlightBuilder.Order.SCORE, field.order());
        assertEquals(256, (int) field.phraseLimit());
        assertTrue(field.requireFieldMatch());
        assertTrue(field.forceSource());
        assertEquals(100, (int) field.noMatchSize());
        assertEquals("fvh", field.highlighterType());
        assertEquals(HighlightBuilder.BoundaryScannerType.WORD, field.boundaryScannerType());
        assertEquals(30, (int) field.boundaryMaxScan());
        assertEquals("span", field.fragmenter());
        assertTrue(field.highlightFilter());
        assertEquals(1000, (int) field.maxAnalyzerOffset());

        String[] preTags = field.preTags();
        assertEquals(1, preTags.length);
        assertEquals("<mark>", preTags[0]);

        String[] postTags = field.postTags();
        assertEquals(1, postTags.length);
        assertEquals("</mark>", postTags[0]);

        // matchedFields is package-private, can't verify directly
    }
}
