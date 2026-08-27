/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.PrimaryTermFieldType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Document input for the Parquet data format.
 *
 * <p>Implements {@link DocumentInput} to collect field-value pairs incrementally during
 * document indexing. Fields are stored as {@link FieldValuePair} objects and later transferred
 * to Arrow vectors by {@link org.opensearch.parquet.vsr.VSRManager#addDocument(ParquetDocumentInput)}.
 *
 * <p>Calling {@link #close()} clears all collected fields and resets the row ID,
 * allowing the instance to be discarded cleanly after use.
 */
public class ParquetDocumentInput implements DocumentInput<List<FieldValuePair>> {

    private static final Logger logger = LogManager.getLogger(ParquetDocumentInput.class);
    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    private final Set<MappedFieldType> dedup = Collections.newSetFromMap(new IdentityHashMap<>());
    private long rowId = -1;
    private boolean isClosed = false;
    // Nested support: children buffered hierarchically, in parse order. A stack tracks currently-open
    // elements so multi-level nesting (comments -> replies) attaches inner elements to their enclosing
    // element instead of losing them.
    private final List<NestedChild> topLevelChildren = new ArrayList<>();
    private final ArrayDeque<NestedChild> childStack = new ArrayDeque<>();
    // Map support: entries of map-typed fields (e.g. a flat_object's attributes) emitted at the document
    // root (not inside any nested element). Keyed by the map field's full name; each entry is one
    // (key,value) pair, preserved in parse order.
    private final LinkedHashMap<String, List<Map.Entry<String, Object>>> topLevelMapEntries = new LinkedHashMap<>();

    /**
     * One nested array element: its full dotted path (e.g. "comments"), its leaf field
     * values in parse order, any deeper nested elements it contains (e.g. replies), and any
     * map-typed fields (e.g. a flat_object {@code attributes}) buffered as key/value entries.
     */
    public static class NestedChild {
        public final String path;
        public final List<FieldValuePair> fields = new ArrayList<>();
        public final List<NestedChild> children = new ArrayList<>();
        // map field full name -> its (key,value) entries in parse order (one MAP<Utf8,Utf8> per key).
        public final LinkedHashMap<String, List<Map.Entry<String, Object>>> mapEntries = new LinkedHashMap<>();

        NestedChild(String path) {
            this.path = path;
        }
    }

    @Override
    public void startNestedChild(String nestedPath) {
        ensureOpen();
        childStack.push(new NestedChild(nestedPath));
    }

    @Override
    public void endNestedChild() {
        ensureOpen();
        NestedChild finished = childStack.pop();
        if (childStack.isEmpty()) {
            topLevelChildren.add(finished);
        } else {
            childStack.peek().children.add(finished);
        }
    }

    /** Returns the buffered top-level nested elements in parse order. */
    public List<NestedChild> getNestedChildren() {
        return topLevelChildren;
    }

    @Override
    public void addMapEntry(MappedFieldType mapField, String key, Object value) {
        ensureOpen();
        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(key, value);
        // Inside a nested element the map lives in that element's struct; otherwise it is a
        // document-root map column.
        LinkedHashMap<String, List<Map.Entry<String, Object>>> target = childStack.isEmpty()
            ? topLevelMapEntries
            : childStack.peek().mapEntries;
        target.computeIfAbsent(mapField.name(), k -> new ArrayList<>()).add(entry);
    }

    /** Returns the document-root map fields (name -&gt; entries), for MAP columns not inside a nested field. */
    public LinkedHashMap<String, List<Map.Entry<String, Object>>> getTopLevelMapEntries() {
        return topLevelMapEntries;
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        ensureOpen();
        // Fields inside a nested scope are routed to the innermost open element (no dedup —
        // different children legitimately repeat the same field type).
        if (childStack.isEmpty() == false) {
            childStack.peek().fields.add(new FieldValuePair(fieldType, value));
            return;
        }
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
            .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
        if (capabilities.isEmpty() && fieldType != PrimaryTermFieldType.INSTANCE) {
            // nothing to support on this format for this field.
            logger.trace("Ignored to add field: {} {}", fieldType.name(), fieldType.getCapabilityMap());
            return;
        }
        if (dedup.add(fieldType) == false) {
            throw new MapperParsingException(
                "Cannot accept multiple values for field: [" + fieldType.name() + "] of type: [" + fieldType.typeName() + "]."
            );
        }
        collectedFields.add(new FieldValuePair(fieldType, value));
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        ensureOpen();
        this.rowId = rowId;
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        if (!isClosed) {
            assert rowId >= 0 : "Row ID must be set before calling getFinalInput";
            // assertions for parquet primary
            // TODO: once parquet is supported in secondary mode, this assertion would change
            assert getFieldCount(IdFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.NAME) == 1;
            assert getFieldCount(VersionFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.PRIMARY_TERM_NAME) == 1;
        }
        return collectedFields;
    }

    @Override
    public long getFieldCount(String fieldName) {
        return collectedFields.stream().filter(fvp -> fvp.getFieldType().name().equals(fieldName)).count();
    }

    @Override
    public void close() {
        isClosed = true;
        collectedFields.clear();
        topLevelChildren.clear();
        childStack.clear();
        topLevelMapEntries.clear();
        rowId = -1;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Cannot add more fields to a frozen document input");
        }
    }

    /**
     * Returns the row ID assigned to this document.
     *
     * @return the row ID, or -1 if not set
     */
    public long getRowId() {
        return rowId;
    }
}
