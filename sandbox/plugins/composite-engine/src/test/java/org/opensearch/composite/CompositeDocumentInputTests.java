/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.search.Query;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link CompositeDocumentInput}.
 */
public class CompositeDocumentInputTests extends OpenSearchTestCase {

    public void testAddFieldBroadcastsToAllFormats() {
        RecordingDocumentInput primaryInput = new RecordingDocumentInput();
        RecordingDocumentInput secondaryInput = new RecordingDocumentInput();

        DataFormat primaryFormat = mockFormat("lucene", 1, Set.of());
        DataFormat secondaryFormat = mockFormat("parquet", 2, Set.of());

        CompositeDocumentInput composite = new CompositeDocumentInput(primaryFormat, primaryInput, Map.of(secondaryFormat, secondaryInput));

        MappedFieldType keywordField = mockFieldType("keyword");
        composite.addField(keywordField, "value1");

        assertEquals(1, primaryInput.addedFields.size());
        assertEquals(1, secondaryInput.addedFields.size());
    }

    public void testSetRowIdBroadcastsToAllInputs() {
        RecordingDocumentInput primaryInput = new RecordingDocumentInput();
        RecordingDocumentInput secondary1 = new RecordingDocumentInput();
        RecordingDocumentInput secondary2 = new RecordingDocumentInput();

        DataFormat primaryFormat = mockFormat("lucene", 1, Set.of());
        DataFormat secondaryFormat1 = mockFormat("parquet", 2, Set.of());
        DataFormat secondaryFormat2 = mockFormat("arrow", 3, Set.of());

        Map<DataFormat, DocumentInput<?>> secondaries = new HashMap<>();
        secondaries.put(secondaryFormat1, secondary1);
        secondaries.put(secondaryFormat2, secondary2);

        CompositeDocumentInput composite = new CompositeDocumentInput(primaryFormat, primaryInput, secondaries);

        composite.setRowId("_row_id", 42L);

        assertEquals(1, primaryInput.rowIds.size());
        assertEquals(42L, (long) primaryInput.rowIds.get(0));
        assertEquals(1, secondary1.rowIds.size());
        assertEquals(42L, (long) secondary1.rowIds.get(0));
        assertEquals(1, secondary2.rowIds.size());
        assertEquals(42L, (long) secondary2.rowIds.get(0));
    }

    public void testGetFinalInputReturnsNull() {
        CompositeDocumentInput composite = new CompositeDocumentInput(
            mockFormat("lucene", 1, Set.of()),
            new RecordingDocumentInput(),
            Map.of()
        );
        assertNull(composite.getFinalInput());
    }

    public void testGetPrimaryInputReturnsPrimaryDocumentInput() {
        RecordingDocumentInput primaryInput = new RecordingDocumentInput();
        CompositeDocumentInput composite = new CompositeDocumentInput(mockFormat("lucene", 1, Set.of()), primaryInput, Map.of());
        assertSame(primaryInput, composite.getPrimaryInput());
    }

    public void testGetPrimaryFormatReturnsPrimaryDataFormat() {
        DataFormat primaryFormat = mockFormat("lucene", 1, Set.of());
        CompositeDocumentInput composite = new CompositeDocumentInput(primaryFormat, new RecordingDocumentInput(), Map.of());
        assertSame(primaryFormat, composite.getPrimaryFormat());
    }

    public void testGetSecondaryInputsReturnsUnmodifiableMap() {
        DataFormat secondaryFormat = mockFormat("parquet", 2, Set.of());
        RecordingDocumentInput secondaryInput = new RecordingDocumentInput();

        CompositeDocumentInput composite = new CompositeDocumentInput(
            mockFormat("lucene", 1, Set.of()),
            new RecordingDocumentInput(),
            Map.of(secondaryFormat, secondaryInput)
        );

        Map<DataFormat, DocumentInput<?>> secondaries = composite.getSecondaryInputs();
        assertEquals(1, secondaries.size());
        expectThrows(
            UnsupportedOperationException.class,
            () -> secondaries.put(mockFormat("x", 0, Set.of()), new RecordingDocumentInput())
        );
    }

    public void testConstructorRejectsNullPrimaryFormat() {
        expectThrows(NullPointerException.class, () -> new CompositeDocumentInput(null, new RecordingDocumentInput(), Map.of()));
    }

    public void testConstructorRejectsNullPrimaryInput() {
        expectThrows(NullPointerException.class, () -> new CompositeDocumentInput(mockFormat("lucene", 1, Set.of()), null, Map.of()));
    }

    public void testConstructorRejectsNullSecondaryInputs() {
        expectThrows(
            NullPointerException.class,
            () -> new CompositeDocumentInput(mockFormat("lucene", 1, Set.of()), new RecordingDocumentInput(), null)
        );
    }

    // --- helpers ---

    private DataFormat mockFormat(String name, long priority, Set<FieldTypeCapabilities> fields) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return priority;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return fields;
            }
        };
    }

    private MappedFieldType mockFieldType(String typeName) {
        return new MappedFieldType(typeName, true, false, true, TextSearchInfo.NONE, Map.of()) {
            @Override
            public String typeName() {
                return typeName;
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

    /**
     * Simple recording implementation of DocumentInput for test assertions.
     */
    static class RecordingDocumentInput implements DocumentInput<Object> {
        final List<Object> addedFields = new ArrayList<>();
        final List<Long> rowIds = new ArrayList<>();

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            addedFields.add(value);
        }

        @Override
        public void setRowId(String rowIdFieldName, long rowId) {
            rowIds.add(rowId);
        }

        @Override
        public Object getFinalInput() {
            return null;
        }

        @Override
        public void close() {}
    }
}
