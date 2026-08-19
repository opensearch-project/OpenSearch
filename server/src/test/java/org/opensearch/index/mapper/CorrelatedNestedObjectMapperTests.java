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
import org.opensearch.index.IndexSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The {@code correlated} mapping parameter on a {@code nested} object.
 *
 * <p>{@code nested} already declares that an object is an array of records whose fields belong
 * together; {@code correlated: true} keeps that declaration but changes how the group is stored —
 * parallel columns paired by array position instead of a hidden document per element. It is an
 * explicit opt-in precisely because it trades away element-scoped {@code nested} queries, so plain
 * {@code nested} must keep behaving exactly as before.
 */
public class CorrelatedNestedObjectMapperTests extends MapperServiceTestCase {

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true).build();
    }

    /**
     * @param correlated {@code null} omits the parameter entirely, otherwise it is written explicitly.
     *                   The distinction matters: an omitted parameter must not reset an existing value
     *                   on a mapping update.
     */
    private XContentBuilder correlatedEventsMapping(Boolean correlated, String type) throws IOException {
        return mapping(b -> {
            b.startObject("Events");
            b.field("type", type);
            if (correlated != null) {
                b.field("correlated", correlated.booleanValue());
            }
            b.startObject("properties");
            b.startObject("Name").field("type", "keyword").field("multi_value", true).endObject();
            b.startObject("Kind").field("type", "keyword").field("multi_value", true).endObject();
            b.endObject();
            b.endObject();
        });
    }

    /** The group is accepted and each leaf is stamped with the object's full path. */
    public void testCorrelatedNestedStampsChildrenWithGroup() throws IOException {
        MapperService mapperService = createMapperService(pluggableSettings(), correlatedEventsMapping(true, "nested"));

        ObjectMapper events = mapperService.getObjectMapper("Events");
        assertNotNull(events);
        assertTrue("Events should still be a nested object", events.nested().isNested());
        assertTrue(events.correlated());

        assertEquals("Events", mapperService.fieldType("Events.Name").correlationGroup());
        assertEquals("Events", mapperService.fieldType("Events.Kind").correlationGroup());
    }

    /** Fields outside a correlated group carry no stamp, so the write path leaves them alone. */
    public void testUncorrelatedFieldsHaveNoGroup() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("Tags").field("type", "keyword").field("multi_value", true).endObject())
        );
        assertNull(mapperService.fieldType("Tags").correlationGroup());
    }

    /** Round-trips through mapping serialization, so it survives cluster state and index reopen. */
    public void testCorrelatedRoundTripsInMapping() throws IOException {
        MapperService mapperService = createMapperService(pluggableSettings(), correlatedEventsMapping(true, "nested"));

        String serialized = mapperService.documentMapper().mappingSource().toString();

        assertTrue("expected [correlated] in the serialized mapping: " + serialized, serialized.contains("\"correlated\":true"));
        assertTrue("expected the object to stay nested: " + serialized, serialized.contains("\"type\":\"nested\""));
    }

    /**
     * A correlated group parses into the parent document. Building the hidden per-element documents
     * instead would scatter the fields across documents, which is the representation the parameter
     * exists to opt out of.
     */
    public void testCorrelatedNestedDoesNotCreateNestedDocuments() throws IOException {
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), correlatedEventsMapping(true, "nested"));

        ParsedDocument doc = mapper.parse(source("{\"Events\":{\"Name\":[\"a\",\"b\"],\"Kind\":[\"k1\",\"k2\"]}}"));

        assertEquals("a correlated group must not add per-element documents", 1, doc.docs().size());
        assertEquals(List.of("a", "b"), indexedValues(doc, "Events.Name"));
        assertEquals(List.of("k1", "k2"), indexedValues(doc, "Events.Kind"));
    }

    /** Plain {@code nested} is untouched: it still produces a document per element. */
    public void testPlainNestedStillCreatesNestedDocuments() throws IOException {
        DocumentMapper mapper = createDocumentMapper(pluggableSettings(), correlatedEventsMapping(false, "nested"));

        ParsedDocument doc = mapper.parse(source("{\"Events\":[{\"Name\":\"a\",\"Kind\":\"k1\"},{\"Name\":\"b\",\"Kind\":\"k2\"}]}"));

        assertTrue("plain nested should still build per-element documents, got " + doc.docs().size(), doc.docs().size() > 1);
    }

    /**
     * Rejected on a plain object: {@code nested} is what declares the grouping, so allowing
     * {@code correlated} without it would be a second, redundant way to say the same thing.
     */
    public void testCorrelatedRequiresNestedType() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(pluggableSettings(), correlatedEventsMapping(true, "object"))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("requires [type: nested]"));
    }

    /**
     * Rejected without a pluggable data format. There it would only remove element-scoped query
     * support, since nested documents already correlate the fields.
     */
    public void testCorrelatedRequiresPluggableDataFormat() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(correlatedEventsMapping(true, "nested"))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("pluggable data format"));
    }

    /** A sub-object has no single array position, so its correlation would be undefined. */
    public void testCorrelatedRejectsSubObjects() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(pluggableSettings(), mapping(b -> {
            b.startObject("Events");
            b.field("type", "nested").field("correlated", true);
            b.startObject("properties");
            b.startObject("Name").field("type", "keyword").field("multi_value", true).endObject();
            b.startObject("Inner").startObject("properties");
            b.startObject("Deep").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject();
            b.endObject();
        })));
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("cannot contain the sub-object"));
    }

    /** It selects the on-disk layout, so flipping it would misdescribe already-written data. */
    public void testCorrelatedIsImmutable() throws IOException {
        MapperService mapperService = createMapperService(pluggableSettings(), correlatedEventsMapping(true, "nested"));

        MapperException e = expectThrows(MapperException.class, () -> merge(mapperService, correlatedEventsMapping(false, "nested")));
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("Cannot update parameter [correlated]"));
    }

    /**
     * Omitting the parameter on a later mapping update leaves it as it was. Treating an absent
     * parameter as {@code false} would silently switch the group's storage layout on any unrelated
     * mapping update, such as adding a field.
     */
    public void testOmittingCorrelatedOnUpdatePreservesIt() throws IOException {
        MapperService mapperService = createMapperService(pluggableSettings(), correlatedEventsMapping(true, "nested"));
        merge(mapperService, correlatedEventsMapping(null, "nested"));

        assertTrue(mapperService.getObjectMapper("Events").correlated());
        assertEquals("Events", mapperService.fieldType("Events.Name").correlationGroup());
    }

    /**
     * The distinct values a field contributed to the root document, in first-seen order. Keyword
     * stores each value twice (indexed term plus doc value) and carries it in {@code binaryValue}
     * rather than {@code stringValue}, so both are accounted for here.
     */
    private static List<String> indexedValues(ParsedDocument doc, String field) {
        Set<String> values = new LinkedHashSet<>();
        for (IndexableField f : doc.rootDoc().getFields(field)) {
            if (f.binaryValue() != null) {
                values.add(f.binaryValue().utf8ToString());
            } else if (f.stringValue() != null) {
                values.add(f.stringValue());
            }
        }
        return new ArrayList<>(values);
    }
}
