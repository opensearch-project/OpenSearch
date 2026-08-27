/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.SeqNoFieldMapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests that {@link ArrowSchemaBuilder#getSchema(MapperService)} maps nested object mappers into
 * {@code LIST<STRUCT>} Arrow columns (rather than flat leaf columns), recursing for nested-in-nested,
 * with struct children sorted by name; while flat (non-nested) fields stay flat and nested leaves are
 * never emitted as top-level columns.
 *
 * <p>Uses {@link MapperServiceTestCase} to build a REAL {@link MapperService}/DocumentMapper from a JSON
 * mapping — the same helper the existing mapper tests use — so the object-mapper nesting metadata that
 * {@code ArrowSchemaBuilder} reads is genuine, not mocked.
 */
public class ArrowSchemaBuilderNestedTests extends MapperServiceTestCase {

    /** Single-level nested: one {@code LIST<STRUCT>} column whose struct children are the nested leaves, name-sorted. */
    public void testSingleLevelNestedBecomesListStruct() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    // Deliberately declared votes-before-author to prove the builder sorts by name.
                    b.startObject("votes").field("type", "integer").endObject();
                    b.startObject("author").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        // Flat field stays a flat column.
        Field title = findTop(schema, "title");
        assertNotNull("flat field must remain a top-level column", title);
        assertTrue(title.getType() instanceof ArrowType.Utf8);

        // Nested leaves are NOT emitted as flat top-level columns.
        assertNull("nested leaf must not be a flat column", findTop(schema, "comments.author"));
        assertNull("nested leaf must not be a flat column", findTop(schema, "comments.votes"));

        // The nested path becomes a LIST<STRUCT> column.
        Field comments = findTop(schema, "comments");
        assertNotNull("nested path must be emitted as a column", comments);
        assertTrue("nested path column is a LIST", comments.getType() instanceof ArrowType.List);

        Field element = onlyChild(comments);
        assertEquals("element", element.getName());
        assertTrue("list element is a STRUCT", element.getType() instanceof ArrowType.Struct);

        // Struct children are the nested leaves, sorted by name: [author, votes].
        assertEquals(List.of("author", "votes"), childNames(element));
        assertTrue(child(element, "author").getType() instanceof ArrowType.Utf8);
        assertTrue(child(element, "votes").getType() instanceof ArrowType.Int);
    }

    /**
     * Nested-in-nested: the inner nested mapper becomes a {@code LIST<STRUCT>} child INSIDE the parent
     * struct (not a separate top-level column), recursively carrying its own leaves.
     */
    public void testNestedInNestedBecomesNestedListStruct() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("author").field("type", "keyword").endObject();
                    b.startObject("replies");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        {
                            b.startObject("text").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        // Only the TOP-LEVEL nested path is a top-level column; the inner one is not.
        assertNotNull(findTop(schema, "comments"));
        assertNull("inner nested path must not be a top-level column", findTop(schema, "comments.replies"));
        assertNull("deep nested leaf must not be a top-level column", findTop(schema, "comments.replies.text"));

        Field commentStruct = onlyChild(findTop(schema, "comments")); // element
        assertTrue(commentStruct.getType() instanceof ArrowType.Struct);
        // Comment struct children, name-sorted: [author, replies].
        assertEquals(List.of("author", "replies"), childNames(commentStruct));

        Field author = child(commentStruct, "author");
        assertTrue("author is a flat struct leaf", author.getType() instanceof ArrowType.Utf8);

        // "replies" is itself a LIST<STRUCT<text>> nested inside the comment struct.
        Field replies = child(commentStruct, "replies");
        assertTrue("nested-in-nested child is a LIST", replies.getType() instanceof ArrowType.List);
        Field replyElement = onlyChild(replies);
        assertTrue(replyElement.getType() instanceof ArrowType.Struct);
        assertEquals(List.of("text"), childNames(replyElement));
        assertTrue(child(replyElement, "text").getType() instanceof ArrowType.Utf8);
    }

    /**
     * A {@code flat_object} inside a nested field (the OTel {@code events.attributes} shape) becomes a
     * {@code MAP<Utf8,Utf8>} child of the element struct — the open key space — instead of exploded
     * dotted leaf columns. Regression guard: before {@code buildMapField} was shared with
     * {@code buildNestedListField}, a nested flat_object produced a MAP type with NO children.
     */
    public void testFlatObjectInNestedBecomesMap() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("events");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").endObject();
                    b.startObject("attributes").field("type", "flat_object").endObject();
                    b.startObject("droppedAttributesCount").field("type", "integer").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        Field element = onlyChild(findTop(schema, "events"));
        assertTrue(element.getType() instanceof ArrowType.Struct);
        // Struct children are name-sorted: [attributes, droppedAttributesCount, name].
        assertEquals(List.of("attributes", "droppedAttributesCount", "name"), childNames(element));

        Field attributes = child(element, "attributes");
        assertTrue("attributes is a MAP", attributes.getType() instanceof ArrowType.Map);
        Field entries = onlyChild(attributes);
        assertEquals("key_value", entries.getName());
        assertTrue("map entries are a STRUCT", entries.getType() instanceof ArrowType.Struct);
        assertEquals(List.of("key", "value"), childNames(entries));
        assertTrue(child(entries, "key").getType() instanceof ArrowType.Utf8);
        assertTrue(child(entries, "value").getType() instanceof ArrowType.Utf8);
        assertFalse("map key must be non-null", child(entries, "key").isNullable());
        assertFalse("map entries struct must be non-null", entries.isNullable());
    }

    /**
     * A top-level {@code flat_object} becomes one {@code MAP<Utf8,Utf8>} column with the canonical
     * {@code key_value} entries group — the same shape the nested case gets.
     */
    public void testTopLevelFlatObjectBecomesMap() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("attributes").field("type", "flat_object").endObject();
        }));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        assertTrue(findTop(schema, "title").getType() instanceof ArrowType.Utf8);

        Field attributes = findTop(schema, "attributes");
        assertNotNull("flat_object must be emitted as a column", attributes);
        assertTrue("flat_object column is a MAP", attributes.getType() instanceof ArrowType.Map);
        Field entries = onlyChild(attributes);
        assertEquals("key_value", entries.getName());
        assertEquals(List.of("key", "value"), childNames(entries));
        assertFalse("map key must be non-null", child(entries, "key").isNullable());
    }

    /** A purely flat mapping produces no LIST columns; every declared leaf stays a flat top-level column. */
    public void testFlatMappingHasNoListColumns() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("count").field("type", "long").endObject();
        }));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        assertNotNull(findTop(schema, "title"));
        assertNotNull(findTop(schema, "count"));
        for (Field f : schema.getFields()) {
            assertFalse("flat mapping must not produce any LIST column: " + f.getName(), f.getType() instanceof ArrowType.List);
        }
    }

    /** The row-id and primary-term metadata columns are always appended regardless of nesting. */
    public void testMetadataColumnsAlwaysPresent() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("author").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        assertNotNull(findTop(schema, DocumentInput.ROW_ID_FIELD));
        assertNotNull(findTop(schema, SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    // --- helpers ---

    /** Returns the top-level field by exact name, or null if absent (Schema.findField throws when absent). */
    private static Field findTop(Schema schema, String name) {
        for (Field f : schema.getFields()) {
            if (f.getName().equals(name)) {
                return f;
            }
        }
        return null;
    }

    private static Field onlyChild(Field field) {
        assertEquals("expected exactly one child of " + field.getName(), 1, field.getChildren().size());
        return field.getChildren().get(0);
    }

    private static List<String> childNames(Field field) {
        return field.getChildren().stream().map(Field::getName).collect(Collectors.toList());
    }

    private static Field child(Field struct, String name) {
        for (Field c : struct.getChildren()) {
            if (c.getName().equals(name)) {
                return c;
            }
        }
        throw new AssertionError("struct " + struct.getName() + " has no child " + name);
    }
}
