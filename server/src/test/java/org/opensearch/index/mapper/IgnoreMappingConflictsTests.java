/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

public class IgnoreMappingConflictsTests extends MapperServiceTestCase {
    private static final Settings TOLERANT = Settings.builder().put(FieldMapper.IGNORE_MAPPING_CONFLICTS_SETTING.getKey(), true).build();

    private DocumentMapper tolerant(XContentBuilder mapping) throws IOException {
        return createDocumentMapper(TOLERANT, mapping);
    }

    private static Set<String> ignored(ParsedDocument document) {
        return Arrays.stream(document.rootDoc().getFields("_ignored")).map(IndexableField::stringValue).collect(Collectors.toSet());
    }

    private void assertPreserved(SourceToParse source, ParsedDocument document) {
        assertEquals(source.source(), document.source());
        assertEquals(source.source().toBytesRef(), document.rootDoc().getBinaryValue("_source"));
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
                b.startObject("outer").startObject("properties");
                b.startObject("bad").field("type", type).startObject("properties");
                b.startObject("value").field("type", "keyword").endObject();
                b.endObject().endObject();
                b.startObject("good").field("type", "keyword").endObject();
                b.endObject().endObject();
            }));
            SourceToParse source = source("{\"outer\":{\"bad\":\"unexpected\",\"good\":\"kept\"}}");
            ParsedDocument doc = mapper.parse(source);
            assertEquals(Set.of("outer.bad"), ignored(doc));
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
            b.startObject("items").field("type", "nested").field("include_in_root", true).startObject("properties");
            b.startObject("price").field("type", "long").endObject();
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        SourceToParse source = source("{\"items\":[{\"price\":1,\"name\":\"first\"},{\"price\":{},\"name\":\"second\"}]}");
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("items.price"), ignored(doc));
        assertEquals(3, doc.docs().size());
        for (ParseContext.Document luceneDoc : doc.docs()) {
            assertNull(luceneDoc.getField("items.price"));
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
            b.startObject("items").field("type", "nested").startObject("properties");
            b.startObject("value").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.startObject("good").field("type", "keyword").endObject();
        }));
        SourceToParse source = source("{\"items\":[{\"value\":\"first\"},\"invalid\"],\"good\":\"kept\"}");
        ParsedDocument doc = mapper.parse(source);
        assertEquals(Set.of("items"), ignored(doc));
        assertEquals(1, doc.docs().size());
        assertEquals("kept", doc.rootDoc().getBinaryValue("good").utf8ToString());
        assertNull(doc.dynamicMappingsUpdate());
        assertPreserved(source, doc);
    }

    public void testDottedFieldBelowScalar() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {
            b.startObject("outer").startObject("properties");
            b.startObject("scalar").field("type", "keyword").endObject();
            b.startObject("good").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        for (String json : new String[] {
            "{\"outer\":{\"scalar.child\":42,\"good\":\"kept\"}}",
            "{\"outer.scalar.child\":42,\"outer.good\":\"kept\"}" }) {
            SourceToParse source = source(json);
            ParsedDocument doc = mapper.parse(source);
            assertEquals(Set.of("outer.scalar.child"), ignored(doc));
            assertEquals("kept", doc.rootDoc().getBinaryValue("outer.good").utf8ToString());
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

    public void testDynamicMappingConflictStillRejectsDocument() throws Exception {
        DocumentMapper mapper = tolerant(mapping(b -> {}));
        expectThrows(IllegalArgumentException.class, () -> mapper.parse(source("{\"field\":[1,\"not-a-number\"]}")));
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

    public void testDefaultStillRejectsConflicts() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"field\":{\"unexpected\":true}}")));
    }

    public void testMalformedJsonAndMetadataAreNotIgnored() throws Exception {
        DocumentMapper mapper = tolerant(fieldMapping(b -> b.field("type", "long")));
        for (String json : new String[] {
            "{\"field\":{\"bad\":}}",
            "{\"field\":[1,]}",
            "{\"field\":\"bad\\q\"}",
            "{\"field\":\"bad\"} {}" }) {
            expectThrows(MapperParsingException.class, () -> mapper.parse(source(json)));
        }
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"_id\":\"forbidden\"}")));
    }

    public void testStrictDynamicMappingIsNotIgnored() throws Exception {
        DocumentMapper mapper = tolerant(topMapping(b -> b.field("dynamic", "strict")));
        expectThrows(MapperParsingException.class, () -> mapper.parse(source("{\"unknown\":42}")));
    }

    public void testNestedDocumentLimitIsNotIgnored() throws Exception {
        Settings settings = Settings.builder().put(TOLERANT).put("index.mapping.nested_objects.limit", 1).build();
        DocumentMapper mapper = createDocumentMapper(settings, mapping(b -> {
            b.startObject("items").field("type", "nested").startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source("{\"items\":[{\"name\":\"one\"},{\"name\":\"two\"}]}"))
        );
        assertThat(e.getMessage(), containsString("nested documents"));
    }

    public void testRequiresCompleteStoredSource() throws Exception {
        DocumentMapper mapper = tolerant(topMapping(b -> b.startObject("_source").field("enabled", false).endObject()));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source("{}")));
        assertThat(e.getMessage(), containsString("complete stored _source"));
    }
}
