/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class IgnoreMappingConflictsTests extends MapperServiceTestCase {
    private static final Settings TOLERANT = Settings.builder().put(FieldMapper.IGNORE_MAPPING_CONFLICTS_SETTING.getKey(), true).build();

    private DocumentMapper tolerant(XContentBuilder mapping) throws IOException {
        return createDocumentMapper(TOLERANT, mapping);
    }

    private static Set<String> ignored(ParsedDocument document) {
        return Arrays.stream(document.rootDoc().getFields("_ignored")).map(IndexableField::stringValue).collect(Collectors.toSet());
    }

    private void assertStoredSource(SourceToParse source, ParsedDocument document) {
        assertEquals(source.source(), document.source());
        assertEquals(source.source().toBytesRef(), document.rootDoc().getBinaryValue("_source"));
    }

    private void assertPreserved(SourceToParse source, ParsedDocument document) {
        assertStoredSource(source, document);
        assertWarnings(
            "Ignored mapping-conflicting fields "
                + ignored(document).stream().sorted().collect(Collectors.toList())
                + " while parsing document with id ["
                + source.id()
                + "] in index [index]; values preserved in _source"
        );
    }

    public void testScalarTypesAtMultipleDepths() throws Exception {
        // Exercise every type deterministically, rather than relying on randomization for coverage.
        for (String type : new String[] {
            "byte",
            "short",
            "integer",
            "long",
            "unsigned_long",
            "half_float",
            "float",
            "double",
            "boolean",
            "date",
            "date_nanos",
            "ip",
            "keyword",
            "text",
            "match_only_text",
            "wildcard",
            "version",
            "binary" }) {
            DocumentMapper mapper = tolerant(mapping(b -> {
                b.startObject("bad").field("type", type);
                if (type.equals("binary")) {
                    b.field("store", true);
                }
                b.endObject();
                b.startObject("outer").startObject("properties");
                b.startObject("inner").startObject("properties");
                b.startObject("bad").field("type", type);
                if (type.equals("binary")) {
                    b.field("store", true);
                }
                b.endObject();
                b.startObject("good").field("type", "keyword").endObject();
                b.endObject().endObject().endObject().endObject();
            }));
            SourceToParse source = source(
                "{\"bad\":{\"unexpected\":true},\"outer\":{\"inner\":{\"bad\":{\"unexpected\":true},\"good\":\"kept\"}}}"
            );
            ParsedDocument doc = mapper.parse(source);
            assertEquals(type, Set.of("bad", "outer.inner.bad"), ignored(doc));
            assertNull(type, doc.rootDoc().getField("bad"));
            assertNull(type, doc.rootDoc().getField("outer.inner.bad"));
            assertEquals(type, "kept", doc.rootDoc().getBinaryValue("outer.inner.good").utf8ToString());
            assertPreserved(source, doc);
        }
    }

    public void testMalformedScalarValues() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("number").field("type", "long").endObject();
            b.startObject("date").field("type", "date").endObject();
            b.startObject("ip").field("type", "ip").endObject();
            b.startObject("flag").field("type", "boolean").endObject();
            b.startObject("binary").field("type", "binary").field("store", true).endObject();
            b.startObject("good").field("type", "keyword").endObject();
        }));
        SourceToParse source = source(
            "{\"number\":\"unknown\",\"date\":\"yesterday-ish\",\"ip\":\"not-an-ip\",\"flag\":\"maybe\",\"binary\":\"!\",\"good\":\"kept\"}"
        );
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("number", "date", "ip", "flag", "binary"), ignored(doc));
        assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
        assertPreserved(source, doc);
    }

    public void testObjectAndNestedReceiveScalars() throws Exception {
        for (String type : new String[] { "object", "nested" }) {
            DocumentMapper mapper = tolerant(mapping(b -> {
                b.startObject("trigger").field("type", "long").endObject();
                b.startObject("outer").startObject("properties");
                b.startObject("bad").field("type", type).startObject("properties");
                b.startObject("value").field("type", "keyword").endObject();
                b.endObject().endObject();
                b.startObject("good").field("type", "keyword").endObject();
                b.endObject().endObject();
            }));
            SourceToParse source = source("{\"trigger\":\"bad\",\"outer\":{\"bad\":\"unexpected\",\"good\":\"kept\"}}");
            ParsedDocument doc = mapper.parse(source);
            assertEquals(Set.of("trigger", "outer.bad"), ignored(doc));
            assertEquals(1, doc.docs().size());
            assertEquals("kept", doc.rootDoc().getBinaryValue("outer.good").utf8ToString());
            assertPreserved(source, doc);
        }
    }

    public void testMixedArrayDropsEntireField() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("bad").field("type", "long").endObject();
            b.startObject("good").field("type", "keyword").endObject();
        }));
        SourceToParse source = source("{\"bad\":[1,[2,\"invalid\"],3],\"good\":\"kept\"}");
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("bad"), ignored(doc));
        assertEquals(0, doc.rootDoc().getFields("bad").length);
        assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
        assertPreserved(source, doc);
    }

    public void testConflictInsideNestedArrayKeepsSiblingFields() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("items").field("type", "nested").field("include_in_root", true).startObject("properties");
            b.startObject("price").field("type", "long").endObject();
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        SourceToParse source = source(
            "{\"trigger\":\"bad\",\"items\":[{\"price\":1,\"name\":\"first\"},{\"price\":\"bad\",\"name\":\"second\"}]}"
        );
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("trigger", "items.price"), ignored(doc));
        assertEquals(2, doc.rootDoc().getFields("_ignored").length);
        assertEquals(3, doc.docs().size());
        for (ParseContext.Document luceneDoc : doc.docs()) {
            assertNull(luceneDoc.getField("items.price"));
        }
        for (int i = 0; i < 2; i++) {
            assertEquals(
                1,
                Arrays.stream(doc.docs().get(i).getFields("items.name"))
                    .filter(field -> field.fieldType().indexOptions() != IndexOptions.NONE)
                    .count()
            );
        }
        assertEquals(
            Set.of("first", "second"),
            Arrays.stream(doc.rootDoc().getFields("items.name"))
                .map(field -> field.binaryValue().utf8ToString())
                .collect(Collectors.toSet())
        );
        assertPreserved(source, doc);
    }

    public void testMixedNestedArrayDropsSubtreeAndNestedDocuments() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("items").field("type", "nested").startObject("properties");
            b.startObject("value").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.startObject("good").field("type", "keyword").endObject();
        }));
        for (String prefix : new String[] { "", "\"trigger\":\"bad\"," }) {
            SourceToParse source = source(
                "{" + prefix + "\"items\":[{\"value\":\"first\"},\"invalid\",{\"value\":\"last\"}],\"good\":\"kept\"}"
            );
            ParsedDocument doc = mapper.parse(source);
            assertEquals(prefix.isEmpty() ? Set.of("items") : Set.of("trigger", "items"), ignored(doc));
            assertEquals(1, doc.docs().size());
            assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
            assertNull(doc.dynamicMappingsUpdate());
            assertPreserved(source, doc);
        }
    }

    public void testDottedFieldBelowScalar() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("outer").startObject("properties");
            b.startObject("scalar").field("type", "keyword").endObject();
            b.startObject("good").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        for (String json : new String[] {
            "{\"trigger\":\"bad\",\"outer\":{\"scalar.child\":42,\"good\":\"kept\"},\"new.parent\":{\"value\":1}}",
            "{\"trigger\":\"bad\",\"outer.scalar.child\":42,\"outer.good\":\"kept\",\"new.parent\":[{\"value\":1}]}",
            "{\"trigger\":\"bad\",\"outer.scalar.child\":{\"value\":42},\"outer.good\":\"kept\",\"new.parent\":{\"value\":1}}",
            "{\"trigger\":\"bad\",\"outer.scalar.child\":[42],\"outer.good\":\"kept\",\"new.parent\":[{\"value\":1}]}" }) {
            SourceToParse source = source(json);
            ParsedDocument doc = mapper.parse(source);
            assertEquals(Set.of("trigger", "outer.scalar.child"), ignored(doc));
            assertEquals("kept", doc.rootDoc().getBinaryValue("outer.good").utf8ToString());
            assertNotNull(doc.rootDoc().getField("new.parent.value"));
            assertNotNull(doc.dynamicMappingsUpdate());
            assertPreserved(source, doc);
        }
    }

    public void testCopyToFailureStillRejectsDocument() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("source").field("type", "keyword").array("copy_to", "first", "number").endObject();
            b.startObject("first").field("type", "keyword").endObject();
            b.startObject("number").field("type", "long").endObject();
            b.startObject("good").field("type", "keyword").endObject();
        }));
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"source\":\"invalid-number\",\"good\":\"kept\"}")));
    }

    public void testMultiFieldFailureStillRejectsDocument() throws Exception {
        DocumentMapper mapper = tolerant(fieldMapping(b -> {
            b.field("type", "keyword").startObject("fields");
            b.startObject("number").field("type", "long").endObject();
            b.endObject();
        }));
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"field\":\"not-a-number\"}")));
    }

    public void testRangeAndGeoPointValues() throws Exception {
        for (String type : new String[] {
            "integer_range",
            "long_range",
            "float_range",
            "double_range",
            "date_range",
            "ip_range",
            "geo_point",
            "flat_object",
            "completion" }) {
            DocumentMapper mapper = tolerant(fieldMapping(b -> {
                b.field("type", type);
                if (type.equals("completion")) {
                    b.field("analyzer", "default").field("search_analyzer", "default");
                }
            }));
            SourceToParse source = source("{\"field\":{\"unexpected\":true}}");
            // flat_object accepts arbitrary object keys by design; a scalar conflicts with its enclosing structure.
            if (type.equals("flat_object")) {
                source = source("{\"field\":42}");
            }
            ParsedDocument doc = mapper.parse(source);
            assertEquals(type, Set.of("field"), ignored(doc));
            assertNull(type, doc.rootDoc().getField("field"));
            assertPreserved(source, doc);
        }
    }

    public void testBinaryAndRangeArraysDoNotLeakKeyedDocValues() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("binary").field("type", "binary").field("doc_values", true).endObject();
            b.startObject("range").field("type", "long_range").endObject();
            b.startObject("good").field("type", "keyword").endObject();
        }));
        SourceToParse source = source(
            "{\"binary\":[\"aGVsbG8=\",\"!\"],\"range\":[{\"gte\":1,\"lte\":2},{\"gte\":\"invalid\"}],\"good\":\"kept\"}"
        );
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("binary", "range"), ignored(doc));
        assertNull(doc.rootDoc().getField("binary"));
        assertNull(doc.rootDoc().getByKey("binary"));
        assertNull(doc.rootDoc().getField("range"));
        assertNull(doc.rootDoc().getByKey("range"));
        assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
        assertPreserved(source, doc);
    }

    public void testDisabledObjectsFlattenedConflict() throws Exception {
        DocumentMapper mapper = tolerant(topMapping(b -> {
            b.field("disable_objects", true).startObject("properties");
            b.startObject("outer.bad").field("type", "long").endObject();
            b.startObject("outer.good").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceToParse source = source("{\"outer\":{\"bad\":\"invalid\",\"good\":\"kept\"}}");
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("outer.bad"), ignored(doc));
        assertNull(doc.rootDoc().getField("outer.bad"));
        assertEquals("kept", doc.rootDoc().getBinaryValue("outer.good").utf8ToString());
        assertPreserved(source, doc);
    }

    public void testLiteralDottedFieldsRemainIndependent() throws Exception {
        DocumentMapper mapper = tolerant(topMapping(b -> {
            b.field("disable_objects", true).startObject("properties");
            b.startObject("a").field("type", "long").endObject();
            b.startObject("a.b").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceToParse source = source("{\"a\":\"bad\",\"a.b\":\"kept\"}");
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("a"), ignored(doc));
        assertEquals("kept", doc.rootDoc().getBinaryValue("a.b").utf8ToString());
        assertPreserved(source, doc);
    }

    public void testDiscoveryFindsAllFlattenedChildrenOfMappedLeaf() throws Exception {
        MapperService service = createMapperService(TOLERANT, topMapping(b -> {
            b.field("disable_objects", true).startObject("properties");
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("a").field("type", "long").endObject();
            b.startObject("a.b").field("type", "long").endObject();
            b.startObject("a.c").field("type", "long").endObject();
            b.startObject("good").field("type", "keyword").endObject();
            b.endObject();
        }));
        for (String value : new String[] { "{\"b\":\"bad\",\"c\":\"bad\"}", "[{\"b\":\"bad\"},{\"c\":\"bad\"}]" }) {
            SourceToParse source = source("{\"trigger\":\"bad\",\"a\":" + value + ",\"good\":\"kept\"}");
            ParsedDocument doc = parseCountingSourcePasses(service, source, 3);
            assertEquals(Set.of("trigger", "a.b", "a.c"), ignored(doc));
            assertNull(doc.rootDoc().getField("a"));
            assertNull(doc.rootDoc().getField("a.b"));
            assertNull(doc.rootDoc().getField("a.c"));
            assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
            assertPreserved(source, doc);
        }
    }

    public void testDefaultStillRejectsConflicts() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"field\":{\"unexpected\":true}}")));
    }

    public void testMalformedJsonAndMetadataAreNotIgnored() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("field").field("type", "long").endObject();
            b.startObject("probe").field("type", "completion").field("analyzer", "default").field("search_analyzer", "default").endObject();
        }));
        for (String json : new String[] {
            "{\"field\":{\"bad\":}}",
            "{\"field\":[1,]}",
            "{\"field\":\"bad\\q\"}",
            "{\"field\":\"bad\"} {}",
            "{\"field\":\"bad\",\"probe\":{\"input\":[\"ok\",",
            "{\"field\":\"bad\",\"probe\":{\"input\":" }) {
            expectThrows(MapperParsingException.class, () -> mapper.parse(source(json)));
        }
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"_id\":\"forbidden\"}")));
    }

    public void testStrictDynamicMappingIsNotIgnored() throws Exception {
        DocumentMapper mapper = tolerant(topMapping(b -> b.field("dynamic", "strict")));
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"unknown\":42}")));
    }

    public void testDiscoveryDoesNotBypassNestedDocumentLimit() throws Exception {
        Settings settings = Settings.builder().put(TOLERANT).put("index.mapping.nested_objects.limit", 1).build();
        DocumentMapper mapper = createDocumentMapper(settings, mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("items").field("type", "nested").startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source("{\"trigger\":\"bad\",\"items\":[{\"name\":\"one\"},{\"name\":\"two\"},\"bad\"]}"))
        );
        assertThat(e.getMessage(), containsString("nested documents"));
        assertNoDeprecationWarnings();
    }

    public void testDiscoveryDoesNotResetArrayDepthLimitInFieldProbe() throws Exception {
        Settings settings = Settings.builder().put(TOLERANT).put("index.mapping.depth.limit", 2).build();
        DocumentMapper mapper = createDocumentMapper(settings, mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("items").startObject("properties");
            b.startObject("price").field("type", "long").endObject();
            b.endObject().endObject();
        }));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source("{\"trigger\":\"bad\",\"items\":[{\"price\":[[1,\"bad\"]]}]}"))
        );
        assertThat(e.getCause().getMessage(), containsString("depth of the nested array field"));
        assertNoDeprecationWarnings();
    }

    public void testRequiresCompleteStoredSource() throws Exception {
        for (String option : new String[] { "enabled", "includes", "excludes" }) {
            Exception e = expectThrows(MapperParsingException.class, () -> tolerant(topMapping(b -> {
                b.startObject("_source");
                if (option.equals("enabled")) {
                    b.field(option, false);
                } else {
                    b.array(option, "field");
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("complete stored _source"));
        }
    }

    public void testMutableDocumentInputIsRejectedBeforeParsing() throws Exception {
        DocumentMapper mapper = tolerant(fieldMapping(b -> b.field("type", "long")));
        DocumentInput<?> input = mock(DocumentInput.class);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"field\":\"bad\"}"), input));
        assertThat(e.getMessage(), containsString("does not support DocumentInput"));
        verifyNoInteractions(input);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testUnsupportedSourceFormatsAreRejected() throws Exception {
        for (String setting : new String[] { "index.derived_source.enabled", "index.pluggable.dataformat.enabled" }) {
            Settings settings = Settings.builder().put(TOLERANT).put(setting, true).build();
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(settings, mapping(b -> {})));
            assertThat(e.getMessage(), containsString("complete stored _source and the Lucene data format"));
        }
    }

    public void testNonInternalContextDoesNotSupportRecovery() {
        ParseContext context = mock(ParseContext.class, CALLS_REAL_METHODS);
        assertFalse(context.isMappingConflictIgnored("field"));
        assertNull(context.mappingConflictsToCollect());
        expectThrows(UnsupportedOperationException.class, () -> context.createFieldProbeContext(null));
    }

    public void testStructuredProbesAcrossSourceFormats() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("point").field("type", "geo_point").endObject();
            b.startObject("suggest")
                .field("type", "completion")
                .field("analyzer", "default")
                .field("search_analyzer", "default")
                .endObject();
        }));
        for (XContentType type : XContentType.values()) {
            try (XContentBuilder b = XContentBuilder.builder(type.xContent())) {
                b.startObject().field("trigger", "bad").array("point", 10.0, 20.0);
                b.startObject("suggest").array("input", "one", "two").endObject().endObject();
                SourceToParse source = new SourceToParse("index", "1", BytesReference.bytes(b), type);
                ParsedDocument doc = mapper.parse(source);
                assertEquals(type.toString(), Set.of("trigger"), ignored(doc));
                assertNotNull(doc.rootDoc().getField("point"));
                assertNotNull(doc.rootDoc().getField("suggest"));
                assertPreserved(source, doc);
            }
        }
    }

    private ParsedDocument parseCountingSourcePasses(MapperService service, SourceToParse source, int expectedPasses) throws Exception {
        DocumentMapperParser mapperParser = spy(service.documentMapperParser());
        DocumentMapper mapper = service.documentMapper();
        ParsedDocument document = new DocumentParser(service.getIndexSettings(), mapperParser, mapper).parseDocument(
            source,
            mapper.mapping().metadataMappers
        );
        verify(mapperParser, times(expectedPasses)).getXContentRegistry();
        return document;
    }

    public void testHealthyDocumentUsesOneSourcePass() throws Exception {
        MapperService service = createMapperService(TOLERANT, mapping(b -> {
            b.startObject("number").field("type", "long").endObject();
            b.startObject("object").startObject("properties");
            b.startObject("good").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        SourceToParse source = source("{\"number\":[1,2],\"object\":{\"good\":\"kept\"}}");
        ParsedDocument doc = parseCountingSourcePasses(service, source, 1);
        assertEquals(Set.of(), ignored(doc));
        assertStoredSource(source, doc);
        assertNoDeprecationWarnings();
    }

    public void testManyConflictsUseThreeSourcePasses() throws Exception {
        // The number of whole-source passes is constant, not proportional to the number of conflicts.
        MapperService service = createMapperService(TOLERANT, mapping(b -> {
            for (int i = 0; i < 32; i++) {
                b.startObject("field" + i).field("type", "long").endObject();
            }
            b.startObject("good").field("type", "keyword").endObject();
        }));
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 32; i++) {
            json.append("\"field").append(i).append("\":\"bad\",");
        }
        json.append("\"good\":\"kept\"}");
        SourceToParse source = source(json.toString());
        ParsedDocument doc = parseCountingSourcePasses(service, source, 3);
        assertEquals(32, ignored(doc).size());
        assertEquals(32, doc.rootDoc().getFields("_ignored").length);
        for (int i = 0; i < 32; i++) {
            assertTrue(ignored(doc).contains("field" + i));
            assertNull(doc.rootDoc().getField("field" + i));
        }
        assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
        assertPreserved(source, doc);
    }

    public void testProbesPreserveNumericTextForDateFormats() throws Exception {
        MapperService service = createMapperService(TOLERANT, mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("clock").field("type", "date").field("format", "HH.mm").endObject();
            b.startObject("window").field("type", "date_range").field("format", "HH.mm").endObject();
        }));
        String values = ",\"clock\":12.30,\"window\":{\"gte\":12.30,\"lte\":13.00}}";
        ParsedDocument healthy = parseCountingSourcePasses(service, source("{\"trigger\":1" + values), 1);
        assertNotNull(healthy.rootDoc().getField("clock"));
        assertNotNull(healthy.rootDoc().getField("window"));
        assertNoDeprecationWarnings();
        SourceToParse source = source("{\"trigger\":\"bad\"" + values);
        ParsedDocument doc = parseCountingSourcePasses(service, source, 3);
        assertEquals(Set.of("trigger"), ignored(doc));
        assertNotNull(doc.rootDoc().getField("clock"));
        assertNotNull(doc.rootDoc().getField("window"));
        assertPreserved(source, doc);
    }

    public void testUnsafeNumericNormalizationFailsRatherThanIgnoringValidField() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("clock").field("type", "date").field("format", "HH.mm").endObject();
        }));
        SourceToParse healthy = new SourceToParse("index", "1", new BytesArray("---\ntrigger: 1\nclock: 12.30\n"), XContentType.YAML);
        assertNotNull(mapper.parse(healthy).rootDoc().getField("clock"));
        assertNoDeprecationWarnings();
        SourceToParse source = new SourceToParse("index", "1", new BytesArray("---\ntrigger: bad\nclock: 12.30\n"), XContentType.YAML);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source));
        assertThat(e.getMessage(), containsString("cannot safely preserve numeric representations"));
        assertNoDeprecationWarnings();
    }

    public void testDiscoveryDefersDocumentOutputValidation() throws Exception {
        MapperService service = createMapperService(TOLERANT, topMapping(b -> {
            b.startObject("_data_stream_timestamp").field("enabled", true).endObject();
            b.startObject("properties");
            b.startObject("trigger").field("type", "long").endObject();
            b.startObject("@timestamp").field("type", "date").endObject();
            b.endObject();
        }));
        SourceToParse source = source("{\"trigger\":\"bad\",\"@timestamp\":\"2026-01-01T00:00:00Z\"}");
        ParsedDocument doc = parseCountingSourcePasses(service, source, 3);
        assertEquals(Set.of("trigger"), ignored(doc));
        assertNotNull(doc.rootDoc().getField("@timestamp"));
        assertPreserved(source, doc);
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> service.documentMapper().parse(source("{\"trigger\":\"bad\"}"))
        );
        assertThat(e.getCause().getMessage(), containsString("timestamp"));
        assertNoDeprecationWarnings();
    }

    public void testFinalDynamicMappingFailureDoesNotStartAnotherRecovery() throws Exception {
        MapperService service = createMapperService(TOLERANT, fieldMapping(b -> b.field("type", "long")));
        DocumentMapperParser mapperParser = spy(service.documentMapperParser());
        DocumentMapper mapper = service.documentMapper();
        DocumentParser parser = new DocumentParser(service.getIndexSettings(), mapperParser, mapper);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parser.parseDocument(source("{\"field\":\"bad\",\"dynamic\":[1,\"bad\"]}"), mapper.mapping().metadataMappers)
        );
        assertThat(e.getMessage(), containsString("mapper [dynamic] cannot be changed from type [long] to [text]"));
        verify(mapperParser, times(3)).getXContentRegistry();
    }

    @TestLogging(value = "org.opensearch.index.mapper.DocumentParser:WARN", reason = "verify conflict recovery warnings")
    public void testRecoveryWarnsOnceWithoutSourceValues() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("a").field("type", "long").endObject();
            b.startObject("b").field("type", "long").endObject();
        }));
        List<String> warnings = new ArrayList<>();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(DocumentParser.class))) {
            appender.addExpectation(new MockLogAppender.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    if (event.getLevel() == Level.WARN && event.getLoggerName().equals(DocumentParser.class.getName())) {
                        warnings.add(event.getMessage().getFormattedMessage());
                    }
                }

                @Override
                public void assertMatched() {
                    assertEquals(1, warnings.size());
                }
            });
            mapper.parse(source("{\"a\":1,\"b\":2}"));
            assertTrue(warnings.isEmpty());
            assertNoDeprecationWarnings();
            expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"a\":\"secret\",\"_id\":\"forbidden\"}")));
            assertTrue(warnings.isEmpty());
            assertNoDeprecationWarnings();
            mapper.parse(source("{\"b\":\"secret\",\"a\":\"secret\"}"));
            appender.assertAllExpectationsMatched();
            assertEquals(
                "Ignored mapping-conflicting fields [a, b] while parsing document with id [1] in index [index]; values preserved in _source",
                warnings.get(0)
            );
            assertWarnings(warnings.get(0));
        }
    }

    public void testDottedObjectConflictPaths() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("a").startObject("properties");
            b.startObject("b").startObject("properties");
            b.startObject("k").field("type", "long").endObject();
            b.endObject().endObject().endObject().endObject();
            b.startObject("good").field("type", "keyword").endObject();
        }));
        for (String json : new String[] {
            "{\"a.b\":1,\"a\":{\"b\":{\"k\":2}},\"good\":\"kept\"}",
            "{\"a\":{\"b\":{\"k\":2}},\"a.b\":1,\"good\":\"kept\"}" }) {
            SourceToParse source = source(json);
            ParsedDocument doc = mapper.parse(source);
            assertEquals(Set.of("a.b"), ignored(doc));
            assertEquals(1, doc.rootDoc().getFields("_ignored").length);
            assertNull(doc.rootDoc().getField("a.b.k"));
            assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
            assertPreserved(source, doc);
        }
        SourceToParse source = source("{\"a\":\"x\",\"a.b\":1,\"good\":\"kept\"}");
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("a"), ignored(doc));
        assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
        assertPreserved(source, doc);
    }

    public void testIgnoreMalformedTakesPrecedenceWithoutDroppingValidOccurrences() throws Exception {
        Settings settings = Settings.builder().put(TOLERANT).put("index.mapping.ignore_malformed", true).build();
        MapperService service = createMapperService(settings, fieldMapping(b -> b.field("type", "long")));
        SourceToParse source = source("{\"field\":[1,\"not_a_number\",2]}");
        ParsedDocument doc = parseCountingSourcePasses(service, source, 1);
        assertEquals(Set.of("field"), ignored(doc));
        assertNotNull(doc.rootDoc().getField("field"));
        assertStoredSource(source, doc);
        assertNoDeprecationWarnings();
    }

    public void testDepthLimitIsNotIgnored() throws Exception {
        Settings settings = Settings.builder().put(TOLERANT).put("index.mapping.depth.limit", 2).build();
        DocumentMapper mapper = createDocumentMapper(settings, mapping(b -> { b.startObject("field").field("type", "long").endObject(); }));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source("{\"field\":\"bad\",\"a\":{\"b\":{\"c\":1}}}"))
        );
        assertThat(e.getCause().getMessage(), containsString("depth"));
    }
}
