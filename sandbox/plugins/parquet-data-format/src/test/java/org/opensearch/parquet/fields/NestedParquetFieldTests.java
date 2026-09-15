/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests {@link NestedParquetField}'s package-visible {@code writeNestedValue} path and its nested-only
 * scalar vector dispatch. A
 * {@code LIST<STRUCT>} vector is hand-built on a plain {@link RootAllocator}, fed via a real
 * {@link ParquetDocumentInput} using the marker-based {@code addField} signal, then read back to verify.
 */
public class NestedParquetFieldTests extends OpenSearchTestCase {

    private RootAllocator testAllocator;
    private NestedParquetField nestedParquetField;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        testAllocator = new RootAllocator();
        nestedParquetField = new NestedParquetField();
    }

    @Override
    public void tearDown() throws Exception {
        testAllocator.close();
        super.tearDown();
    }

    /** A single-level list of two elements: correct offsets and per-element struct-child values. */
    public void testAddToVectorTwoElements() throws Exception {
        KeywordFieldMapper.KeywordFieldType author = withParquetCapability(new KeywordFieldMapper.KeywordFieldType("comments.author"));
        NumberFieldMapper.NumberFieldType votes = withParquetCapability(
            new NumberFieldMapper.NumberFieldType("comments.votes", NumberFieldMapper.NumberType.INTEGER)
        );

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(nestedPathMarker(), "comments");
        doc.addField(author, "alice");
        doc.addField(votes, 3);
        doc.addField(nestedPathMarker(), "comments");
        doc.addField(author, "bob");
        doc.addField(votes, 7);

        flush(doc);
        try (ListVector list = newListOfStruct("comments", List.of(utf8("author"), int32("votes")))) {
            addNestedToVector(list, 0, doc.getNestedChildren());
            list.setValueCount(1); // cascade counts to struct + children for read-back

            assertEquals(0, list.getElementStartIndex(0));
            assertEquals(2, list.getElementEndIndex(0));

            StructVector struct = (StructVector) list.getDataVector();
            VarCharVector authorVec = (VarCharVector) struct.getChild("author");
            IntVector votesVec = (IntVector) struct.getChild("votes");
            assertEquals("alice", authorVec.getObject(0).toString());
            assertEquals("bob", authorVec.getObject(1).toString());
            assertEquals(3, votesVec.get(0));
            assertEquals(7, votesVec.get(1));
        }
    }

    /** Writing a list at a non-zero row index leaves earlier rows empty and starts at the right offset. */
    public void testAddToVectorAtNonZeroRow() throws Exception {
        KeywordFieldMapper.KeywordFieldType author = withParquetCapability(new KeywordFieldMapper.KeywordFieldType("comments.author"));
        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(nestedPathMarker(), "comments");
        doc.addField(author, "carol");

        flush(doc);
        try (ListVector list = newListOfStruct("comments", List.of(utf8("author")))) {
            // Row 0 left untouched (empty list); write the single element at row 1.
            addNestedToVector(list, 1, doc.getNestedChildren());
            list.setValueCount(2);

            assertTrue("row 0 is empty/null", list.isNull(0) || list.getObject(0) == null || list.getObject(0).isEmpty());
            assertEquals("row 1 has exactly one element", 1, list.getObject(1).size());
            int start = list.getElementStartIndex(1);
            VarCharVector authorVec = (VarCharVector) ((StructVector) list.getDataVector()).getChild("author");
            assertEquals("carol", authorVec.getObject(start).toString());
        }
    }

    /** Nested-in-nested: one comment with two replies — recurses into the inner {@code LIST<STRUCT<text>>}. */
    public void testAddToVectorRecursesIntoInnerList() throws Exception {
        KeywordFieldMapper.KeywordFieldType author = withParquetCapability(new KeywordFieldMapper.KeywordFieldType("comments.author"));
        KeywordFieldMapper.KeywordFieldType replyText = withParquetCapability(
            new KeywordFieldMapper.KeywordFieldType("comments.replies.text")
        );

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(nestedPathMarker(), "comments");
        doc.addField(author, "alice");
        doc.addField(nestedPathMarker(), "comments.replies");
        doc.addField(replyText, "first");
        doc.addField(nestedPathMarker(), "comments.replies");
        doc.addField(replyText, "second");

        // comment struct = { author: utf8, replies: LIST<STRUCT<text>> }
        Field repliesChild = new Field(
            "replies",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(structElement(List.of(utf8("text"))))
        );
        flush(doc);
        try (ListVector comments = newListOfStructRaw("comments", List.of(utf8("author"), repliesChild))) {
            addNestedToVector(comments, 0, doc.getNestedChildren());
            comments.setValueCount(1);

            assertEquals("one comment", 1, comments.getObject(0).size());
            StructVector commentStruct = (StructVector) comments.getDataVector();
            assertEquals("alice", ((VarCharVector) commentStruct.getChild("author")).getObject(0).toString());

            ListVector replies = (ListVector) commentStruct.getChild("replies");
            assertEquals("two replies nested in the comment", 2, replies.getElementEndIndex(0) - replies.getElementStartIndex(0));
            VarCharVector textVec = (VarCharVector) ((StructVector) replies.getDataVector()).getChild("text");
            int rstart = replies.getElementStartIndex(0);
            assertEquals("first", textVec.getObject(rstart).toString());
            assertEquals("second", textVec.getObject(rstart + 1).toString());
        }
    }

    /** Vector dispatch covering every scalar type supported as a nested leaf. */
    public void testNestedLeafTypeDispatch() throws Exception {
        try (VarCharVector v = new VarCharVector("s", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, "hello");
            v.setValueCount(1);
            assertEquals("hello", v.getObject(0).toString());
        }
        try (IntVector v = new IntVector("i", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, 42);
            v.setValueCount(1);
            assertEquals(42, v.get(0));
        }
        try (BigIntVector v = new BigIntVector("l", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, 42L);
            v.setValueCount(1);
            assertEquals(42L, v.get(0));
        }
        try (Float8Vector v = new Float8Vector("d", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, 3.5d);
            v.setValueCount(1);
            assertEquals(3.5d, v.get(0), 0.0);
        }
        try (Float4Vector v = new Float4Vector("f", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, 2.5f);
            v.setValueCount(1);
            assertEquals(2.5f, v.get(0), 0.0f);
        }
        try (BitVector v = new BitVector("b", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, Boolean.TRUE);
            v.setValueCount(1);
            assertEquals(1, v.get(0));
        }
        // half_float — matches HalfFloatParquetField's own top-level conversion (same instance, since
        // there is only one implementation now, not a separate copy for nested).
        try (Float2Vector v = new Float2Vector("hf", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, 1.5f);
            v.setValueCount(1);
            assertEquals(1.5f, v.getValueAsFloat(0), 0.0f);
        }
        // unsigned_long
        try (UInt8Vector v = new UInt8Vector("ul", testAllocator)) {
            v.allocateNew();
            writeLeafValue(v, 0, 9_000_000_000L);
            v.setValueCount(1);
            assertEquals(9_000_000_000L, v.get(0));
        }
        // binary — a raw byte[]
        try (VarBinaryVector v = new VarBinaryVector("bin", testAllocator)) {
            v.allocateNew();
            byte[] raw = { 1, 2, 3 };
            writeLeafValue(v, 0, raw);
            v.setValueCount(1);
            assertArrayEquals(raw, v.get(0));
        }
        // ip — an InetAddress, point-encoded
        try (VarBinaryVector v = new VarBinaryVector("ip", testAllocator)) {
            v.allocateNew();
            InetAddress address = InetAddress.getByName("192.168.1.1");
            writeLeafValue(v, 0, address);
            v.setValueCount(1);
            assertArrayEquals(InetAddressPoint.encode(address), v.get(0));
        }
    }

    private static void writeLeafValue(FieldVector vector, int index, Object value) {
        NestedParquetField.writeLeafValue(vector, index, value);
    }

    /** MAP-in-STRUCT: every dynamic key of every element survives, and a no-attributes element writes null. */
    public void testAddToVectorMapPreservesAllDynamicKeys() throws Exception {
        KeywordFieldMapper.KeywordFieldType name = withParquetCapability(new KeywordFieldMapper.KeywordFieldType("events.name"));
        MappedFieldType attrs = flatObjectFieldType("events.attributes");

        ParquetDocumentInput doc = new ParquetDocumentInput();
        // e0: two keys
        doc.addField(nestedPathMarker(), "events");
        doc.addField(name, "e0");
        doc.addField(attrs, mapEntry("http.method", "GET"));
        doc.addField(attrs, mapEntry("http.status", "200"));
        // e1: a completely different key
        doc.addField(nestedPathMarker(), "events");
        doc.addField(name, "e1");
        doc.addField(attrs, mapEntry("db.system", "postgres"));
        // e2: no attributes at all
        doc.addField(nestedPathMarker(), "events");
        doc.addField(name, "e2");

        flush(doc);
        try (ListVector events = newListOfStruct("events", List.of(utf8("name"), mapField("attributes")))) {
            addNestedToVector(events, 0, doc.getNestedChildren());
            events.setValueCount(1);

            assertEquals(0, events.getElementStartIndex(0));
            assertEquals(3, events.getElementEndIndex(0));

            StructVector struct = (StructVector) events.getDataVector();
            VarCharVector nameVec = (VarCharVector) struct.getChild("name");
            assertEquals("e0", nameVec.getObject(0).toString());
            assertEquals("e1", nameVec.getObject(1).toString());
            assertEquals("e2", nameVec.getObject(2).toString());

            MapVector mapVec = (MapVector) struct.getChild("attributes");
            // Offsets must be contiguous across all three elements: [0,2) [2,3) [3,3)
            assertEquals(0, mapVec.getElementStartIndex(0));
            assertEquals(2, mapVec.getElementEndIndex(0));
            assertEquals(2, mapVec.getElementStartIndex(1));
            assertEquals(3, mapVec.getElementEndIndex(1));
            assertEquals(3, mapVec.getElementStartIndex(2));
            assertEquals(3, mapVec.getElementEndIndex(2));
            assertTrue("no entries writes an explicit null map, not an empty one", mapVec.isNull(2));

            StructVector entries = (StructVector) mapVec.getDataVector();
            VarCharVector keys = (VarCharVector) entries.getChild(MapVector.KEY_NAME);
            VarCharVector values = (VarCharVector) entries.getChild(MapVector.VALUE_NAME);
            assertEquals("http.method", keys.getObject(0).toString());
            assertEquals("GET", values.getObject(0).toString());
            assertEquals("http.status", keys.getObject(1).toString());
            assertEquals("200", values.getObject(1).toString());
            // e1's distinct dynamic key survived rather than being dropped to match e0's key set.
            assertEquals("db.system", keys.getObject(2).toString());
            assertEquals("postgres", values.getObject(2).toString());
        }
    }

    /** Duplicate keys are preserved: a parquet MAP is a repeated group, so {@code {"a":[1,2]}} keeps both. */
    public void testAddToVectorMapPreservesDuplicateKeys() throws Exception {
        MappedFieldType attrs = flatObjectFieldType("events.attributes");
        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(nestedPathMarker(), "events");
        doc.addField(attrs, mapEntry("a", "1"));
        doc.addField(attrs, mapEntry("a", "2"));

        flush(doc);
        try (ListVector events = newListOfStruct("events", List.of(mapField("attributes")))) {
            addNestedToVector(events, 0, doc.getNestedChildren());
            events.setValueCount(1);

            MapVector mapVec = (MapVector) ((StructVector) events.getDataVector()).getChild("attributes");
            assertEquals(2, mapVec.getElementEndIndex(0) - mapVec.getElementStartIndex(0));
            StructVector entries = (StructVector) mapVec.getDataVector();
            VarCharVector keys = (VarCharVector) entries.getChild(MapVector.KEY_NAME);
            VarCharVector values = (VarCharVector) entries.getChild(MapVector.VALUE_NAME);
            assertEquals("a", keys.getObject(0).toString());
            assertEquals("a", keys.getObject(1).toString());
            assertEquals("1", values.getObject(0).toString());
            assertEquals("2", values.getObject(1).toString());
        }
    }

    // --- helpers ---

    /**
     * Sets a Parquet-owning capability map, mimicking real mapping-creation-time assignment —
     * {@code ParquetDocumentInput.addField} checks this for every field, nested leaves included, so a
     * bare field type built directly here needs it set explicitly or it's silently dropped.
     */
    private static <T extends MappedFieldType> T withParquetCapability(T fieldType) {
        fieldType.setCapabilityMap(
            Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE))
        );
        return fieldType;
    }

    /** A field type reporting {@code typeName() == "flat_object"}, still needing {@link #withParquetCapability}. */
    private static MappedFieldType flatObjectFieldType(String name) {
        return withParquetCapability(new FlatObjectFieldMapper.FlatObjectFieldType(name, null, true, true));
    }

    private static Map.Entry<String, Object> mapEntry(String key, Object value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    /** A field type reporting {@code typeName() == NestedPathFieldMapper.NAME} — that has no public constructor. */
    private static MappedFieldType nestedPathMarker() {
        return new MappedFieldType(NestedPathFieldMapper.NAME, true, false, false, TextSearchInfo.NONE, Map.of()) {
            @Override
            public String typeName() {
                return NestedPathFieldMapper.NAME;
            }

            @Override
            public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
                return null;
            }

            @Override
            public Query termQuery(Object value, QueryShardContext context) {
                return null;
            }
        };
    }

    private void addNestedToVector(ListVector list, int rowIndex, List<ParquetDocumentInput.NestedChild> children) {
        nestedParquetField.writeNestedValue(list, rowIndex, children);
    }

    /** Closes open nested elements through the normal field-routing path. */
    private static void flush(ParquetDocumentInput doc) {
        doc.addField(withParquetCapability(new KeywordFieldMapper.KeywordFieldType("test_flush")), "flush");
    }

    private static Field utf8(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    /** Mirrors {@code FlatObjectParquetField.buildField}: {@code MAP<Utf8,Utf8>} with a {@code key_value} struct. */
    private static Field mapField(String name) {
        Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
        Field value = new Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field entries = new Field("key_value", new FieldType(false, ArrowType.Struct.INSTANCE, null), List.of(key, value));
        return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
    }

    private static Field int32(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
    }

    private static Field structElement(List<Field> structChildren) {
        return new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), structChildren);
    }

    /** Creates and allocates a {@code LIST<STRUCT>} vector named {@code path}, struct children sorted by name. */
    private ListVector newListOfStruct(String path, List<Field> structChildren) {
        List<Field> sorted = new ArrayList<>(structChildren);
        sorted.sort(Comparator.comparing(Field::getName));
        return newListOfStructRaw(path, sorted);
    }

    /** Creates and allocates a {@code LIST<STRUCT>} vector named {@code path}, preserving struct-child order. */
    private ListVector newListOfStructRaw(String path, List<Field> structChildren) {
        Field field = new Field(path, FieldType.nullable(ArrowType.List.INSTANCE), List.of(structElement(structChildren)));
        ListVector vector = (ListVector) field.createVector(testAllocator);
        vector.allocateNew();
        return vector;
    }
}
