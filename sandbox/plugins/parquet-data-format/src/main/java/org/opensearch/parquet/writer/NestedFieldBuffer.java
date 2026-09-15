/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Collects nested field elements while a document is parsed. */
final class NestedFieldBuffer {

    private final List<ParquetDocumentInput.NestedChild> topLevelChildren = new ArrayList<>();
    private final ArrayDeque<ParquetDocumentInput.NestedChild> childStack = new ArrayDeque<>();

    /** Starts the next nested element, closing any sibling or completed ancestor scopes first. */
    void startElement(String path) {
        closeElementsNotOwning(path);
        childStack.push(new ParquetDocumentInput.NestedChild(path));
    }

    /** Routes a map entry to the current nested element, or returns false when it belongs at the root. */
    boolean addMapEntry(MappedFieldType fieldType, Map.Entry<String, Object> entry) {
        closeElementsNotOwning(fieldType.name());
        if (childStack.isEmpty()) {
            return false;
        }
        childStack.peek().mapEntries.computeIfAbsent(fieldType.name(), ignored -> new ArrayList<>()).add(entry);
        return true;
    }

    /** Routes a scalar leaf to the current nested element, or returns false when it belongs at the root. */
    boolean addLeaf(MappedFieldType fieldType, Object value) {
        closeElementsNotOwning(fieldType.name());
        if (childStack.isEmpty()) {
            return false;
        }

        ParquetDocumentInput.NestedChild current = childStack.peek();
        String relativeName = fieldType.name().substring(current.path.length() + 1);
        for (ParquetDocumentInput.NestedLeaf existingLeaf : current.fields) {
            if (existingLeaf.name.equals(relativeName)) {
                throw new MapperParsingException(
                    "Cannot accept multiple values for field: [" + fieldType.name() + "] of type: [" + fieldType.typeName() + "]."
                );
            }
        }
        current.fields.add(new ParquetDocumentInput.NestedLeaf(relativeName, fieldType, value));
        return true;
    }

    /** Returns completed top-level elements in parse order. */
    List<ParquetDocumentInput.NestedChild> children() {
        return topLevelChildren;
    }

    /** Completes every scope still open at the end of document parsing. */
    void finish() {
        while (childStack.isEmpty() == false) {
            closeTopElement();
        }
    }

    /** Clears all per-document nested state. */
    void clear() {
        topLevelChildren.clear();
        childStack.clear();
    }

    private void closeElementsNotOwning(String name) {
        while (childStack.isEmpty() == false && name.startsWith(childStack.peek().path + ".") == false) {
            closeTopElement();
        }
    }

    private void closeTopElement() {
        ParquetDocumentInput.NestedChild finished = childStack.pop();
        if (childStack.isEmpty()) {
            topLevelChildren.add(finished);
        } else {
            childStack.peek().children.add(finished);
        }
    }
}
