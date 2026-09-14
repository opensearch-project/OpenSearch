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
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
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
 * <p>{@code nested} and {@code flat_object} data arrive through the SAME generic {@link #addField}
 * every other field uses — there is no nested-specific method on the {@link DocumentInput} SPI. Two
 * signals are recognized specially, by inspecting the field being added, entirely inside this class:
 * <ul>
 *   <li>a field whose {@link MappedFieldType#typeName()} is {@link NestedPathFieldMapper#NAME} is the
 *       "a new nested array element starts here" marker emitted once per element by
 *       {@code DocumentParser#nestedContext}; its value is the element's full dotted path.</li>
 *   <li>a {@code flat_object} field whose value is a {@link Map.Entry} is one flattened (key, value)
 *       pair of that field's open key space, emitted per-leaf by {@code FlatObjectFieldMapper}.</li>
 * </ul>
 * Both signals feed the same element tree ({@link #childStack}/{@link #topLevelChildren}) that
 * {@link #addField} builds for ordinary nested leaves — see {@link #closeElementsNotOwning} for how
 * the tree closes as the parse walk moves between scopes, with no explicit "end of element" signal.
 *
 * <p>Calling {@link #close()} clears all collected fields and resets the row ID,
 * allowing the instance to be discarded cleanly after use.
 */
public class ParquetDocumentInput implements DocumentInput<List<FieldValuePair>> {

    private static final Logger logger = LogManager.getLogger(ParquetDocumentInput.class);
    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    // Keyed by field name, not field-type identity: within a single document parse each logical
    // field (including the derived-source `_ignored_source.*` companion) has a unique name, while
    // identity would silently miss a match if the parser ever handed back a fresh wrapper per array
    // element — degrading a multi_value field to last-value-wins or bypassing the scalar duplicate
    // guard. Name keying makes accumulation robust to that.
    private final Map<String, FieldValuePair> seen = new HashMap<>();
    private long rowId = -1;
    private boolean isClosed = false;
    // Nested support: children buffered hierarchically, in parse order. A stack tracks currently-open
    // elements so multi-level nesting (comments -> replies) attaches inner elements to their enclosing
    // element instead of losing them. There is no explicit "close" signal — the stack is closed lazily,
    // driven by the next marker/leaf/flush that no longer belongs to the open scope; see
    // closeElementsNotOwning.
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
        public final List<NestedLeaf> fields = new ArrayList<>();
        public final List<NestedChild> children = new ArrayList<>();
        // map field full name -> its (key,value) entries in parse order (one MAP<Utf8,Utf8> per key).
        public final LinkedHashMap<String, List<Map.Entry<String, Object>>> mapEntries = new LinkedHashMap<>();

        NestedChild(String path) {
            this.path = path;
        }
    }

    /**
     * One leaf field of a nested element, with its name already relative to the element's own
     * struct — computed once here, while the current nested scope's path is on hand, rather than
     * re-derived from the full dotted {@code fieldType.name()} at write time.
     */
    public static class NestedLeaf {
        public final String name;
        public final MappedFieldType fieldType;
        public final Object value;

        NestedLeaf(String name, MappedFieldType fieldType, Object value) {
            this.name = name;
            this.fieldType = fieldType;
            this.value = value;
        }
    }

    /** Returns the buffered top-level nested elements in parse order. */
    public List<NestedChild> getNestedChildren() {
        return topLevelChildren;
    }

    /** Returns the document-root map fields (name -&gt; entries), for MAP columns not inside a nested field. */
    public LinkedHashMap<String, List<Map.Entry<String, Object>>> getTopLevelMapEntries() {
        return topLevelMapEntries;
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        ensureOpen();
        // The "a new nested array element starts here" marker — recognized by name, BEFORE the
        // capability check below, since this is bookkeeping internal to this class rather than
        // something any format "claims" a capability for. Every other DocumentInput (e.g. Lucene) that
        // doesn't special-case this field name just applies its own ordinary capability self-filter to
        // it, same as any other field.
        if (NestedPathFieldMapper.NAME.equals(fieldType.typeName())) {
            String elementPath = (String) value;
            closeElementsNotOwning(elementPath);
            childStack.push(new NestedChild(elementPath));
            return;
        }
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
            .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
        if (capabilities.isEmpty() && fieldType != PrimaryTermFieldType.INSTANCE) {
            // nothing to support on this format for this field.
            logger.trace("Ignored to add field: {} {}", fieldType.name(), fieldType.getCapabilityMap());
            return;
        }
        // Close out any open elements this field doesn't belong to (moving to a sibling/ancestor scope,
        // or back out to the document root) before routing it. No-op while the stack is empty.
        closeElementsNotOwning(fieldType.name());
        if (value instanceof Map.Entry<?, ?> && FlatObjectFieldMapper.CONTENT_TYPE.equals(fieldType.typeName())) {
            // One flattened (key, value) pair of a flat_object's open key space. Lives in the innermost
            // open element's map, or at the document root if none is open.
            @SuppressWarnings("unchecked")
            Map.Entry<String, Object> entry = (Map.Entry<String, Object>) value;
            LinkedHashMap<String, List<Map.Entry<String, Object>>> target = childStack.isEmpty()
                ? topLevelMapEntries
                : childStack.peek().mapEntries;
            target.computeIfAbsent(fieldType.name(), k -> new ArrayList<>()).add(entry);
            return;
        }
        // Ordinary fields inside a nested scope route to the innermost open element. No dedup across
        // different elements, but within the SAME element a repeated leaf must be rejected — the second
        // writeValue would silently overwrite the first at the same struct index otherwise.
        if (childStack.isEmpty() == false) {
            NestedChild current = childStack.peek();
            String relativeName = fieldType.name().substring(current.path.length() + 1);
            for (NestedLeaf existingLeaf : current.fields) {
                if (existingLeaf.name.equals(relativeName)) {
                    throw new MapperParsingException(
                        "Cannot accept multiple values for field: [" + fieldType.name() + "] of type: [" + fieldType.typeName() + "]."
                    );
                }
            }
            current.fields.add(new NestedLeaf(relativeName, fieldType, value));
            return;
        }
        FieldValuePair existing = seen.get(fieldType.name());
        if (existing == null) {
            // Fields declared `multi_value: true` in the mapping start out as a list of one so the
            // value shape reaching the VSR is the same whether the document had one value or several.
            // An explicit empty array (`"field": []`) is signalled by an empty List and seeds a
            // zero-value pair, so its LIST cell is written empty-but-non-null rather than null.
            final FieldValuePair pair;
            if (fieldType.isMultiValued()) {
                pair = value instanceof List<?> list && list.isEmpty()
                    ? FieldValuePair.emptyMultiValued(fieldType)
                    : FieldValuePair.multiValued(fieldType, value);
            } else {
                pair = new FieldValuePair(fieldType, value);
            }
            seen.put(fieldType.name(), pair);
            collectedFields.add(pair);
            return;
        }
        if (existing.isMultiValued() == false) {
            if (fieldType.isMultiValueSupported() && fieldType.isMultiValueAutoPromotionEnabled()) {
                existing.promoteToMultiValued(value);
                return;
            }
            String reason = fieldType.isMultiValueSupported()
                ? "the field is locked scalar by [multi_value: false]"
                : "the field type does not support automatic multi-value promotion";
            throw new MapperParsingException(
                "Cannot accept multiple values for field: [" + fieldType.name() + "] of type: [" + fieldType.typeName() + "]: " + reason
            );
        }
        existing.addValue(value);
    }

    /**
     * Closes open elements from the innermost outward until either the stack is empty or the new top's
     * path is a proper dotted prefix of {@code name} — i.e. {@code name} is that element or something
     * inside it. An element never owns itself, so a sibling/ancestor marker always closes at least the
     * current element.
     */
    private void closeElementsNotOwning(String name) {
        while (childStack.isEmpty() == false && name.startsWith(childStack.peek().path + ".") == false) {
            closeTopElement();
        }
    }

    /** Pops the innermost open element and attaches it to its parent (or to the top level if none). */
    private void closeTopElement() {
        NestedChild finished = childStack.pop();
        if (childStack.isEmpty()) {
            topLevelChildren.add(finished);
        } else {
            childStack.peek().children.add(finished);
        }
    }

    /** Closes out every still-open element — called once parsing is done and no further signal will arrive. */
    void flushOpenElements() {
        while (childStack.isEmpty() == false) {
            closeTopElement();
        }
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        ensureOpen();
        this.rowId = rowId;
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        // No explicit "end of element" signal is emitted for the last-open element(s) of the document
        // (unlike a leaf/marker arriving afterward, nothing arrives to drive closeElementsNotOwning) —
        // flush them here, before the caller (VSRManager) reads getNestedChildren().
        flushOpenElements();
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
        // Counts values, not entries: a multi-valued field is one entry holding N values, and
        // callers (single-value assertions below, the data-stream @timestamp check) mean values.
        //
        // O(1) via the name index: addField routes every value for a name into the single pair
        // registered under that name in `seen`, so `seen` and `collectedFields` always hold the
        // same pairs and the lookup is exact. This is on the per-value hot path — the mapper calls
        // it before every AUTO-state keyword value to decide on scalar-to-LIST promotion — so a
        // linear scan of collectedFields here made document parsing quadratic in the field count.
        FieldValuePair pair = seen.get(fieldName);
        return pair == null ? 0 : pair.valueCount();
    }

    @Override
    public void close() {
        flushOpenElements();
        isClosed = true;
        collectedFields.clear();
        seen.clear();
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
