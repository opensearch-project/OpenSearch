/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class FieldCapabilityAssignerTests extends OpenSearchTestCase {

    private static final DataFormat PARQUET_FORMAT = new MockDataFormat(
        "parquet",
        0,
        Set.of(
            new FieldTypeCapabilities(
                "integer",
                Set.of(
                    FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
                    FieldTypeCapabilities.Capability.BLOOM_FILTER,
                    FieldTypeCapabilities.Capability.POINT_RANGE
                )
            ),
            new FieldTypeCapabilities(
                "long",
                Set.of(
                    FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
                    FieldTypeCapabilities.Capability.BLOOM_FILTER,
                    FieldTypeCapabilities.Capability.POINT_RANGE
                )
            ),
            new FieldTypeCapabilities(
                "keyword",
                Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER)
            ),
            new FieldTypeCapabilities(
                "date",
                Set.of(
                    FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
                    FieldTypeCapabilities.Capability.BLOOM_FILTER,
                    FieldTypeCapabilities.Capability.POINT_RANGE
                )
            ),
            new FieldTypeCapabilities(
                "_seq_no",
                Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.POINT_RANGE)
            )
        )
    );

    private static final DataFormat LUCENE_FORMAT = new MockDataFormat(
        "lucene",
        0,
        Set.of(
            new FieldTypeCapabilities(
                "integer",
                Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS, FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)
            ),
            new FieldTypeCapabilities(
                "long",
                Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS, FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)
            ),
            new FieldTypeCapabilities(
                "keyword",
                Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH, FieldTypeCapabilities.Capability.STORED_FIELDS)
            ),
            new FieldTypeCapabilities(
                "date",
                Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS, FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)
            ),
            new FieldTypeCapabilities(
                "_seq_no",
                Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.POINT_RANGE)
            )
        )
    );

    public void testAssignSingleFormatCoversAllCapabilities() {
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(List.of(PARQUET_FORMAT, LUCENE_FORMAT));

        // Integer field: isSearchable=true (POINT_RANGE), hasDocValues=true (COLUMNAR_STORAGE)
        NumberFieldMapper.NumberFieldType intField = new NumberFieldMapper.NumberFieldType("my_int", NumberFieldMapper.NumberType.INTEGER);
        assigner.assign(intField);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = intField.getCapabilityMap();
        assertFalse("Capability map should not be empty", capMap.isEmpty());
        assertTrue(
            "Parquet should own COLUMNAR_STORAGE",
            capMap.get(PARQUET_FORMAT).contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
        );
        assertTrue("Parquet should own POINT_RANGE", capMap.get(PARQUET_FORMAT).contains(FieldTypeCapabilities.Capability.POINT_RANGE));
    }

    public void testAssignKeywordFieldSplitBetweenFormats() {
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(List.of(PARQUET_FORMAT, LUCENE_FORMAT));

        // Keyword: isSearchable=true (FULL_TEXT_SEARCH), hasDocValues=true (COLUMNAR_STORAGE), isStored=false
        KeywordFieldMapper.KeywordFieldType keywordField = new KeywordFieldMapper.KeywordFieldType("my_keyword");
        assigner.assign(keywordField);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = keywordField.getCapabilityMap();
        assertFalse("Capability map should not be empty", capMap.isEmpty());
        // Parquet is first in priority and supports COLUMNAR_STORAGE for keyword
        assertTrue(
            "Parquet should own COLUMNAR_STORAGE for keyword",
            capMap.get(PARQUET_FORMAT).contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
        );
        // Lucene should handle FULL_TEXT_SEARCH since Parquet doesn't declare it for keyword
        assertTrue(
            "Lucene should own FULL_TEXT_SEARCH for keyword",
            capMap.get(LUCENE_FORMAT).contains(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)
        );
    }

    public void testAssignWithNoFormatsProducesEmptyMap() {
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(List.of());

        NumberFieldMapper.NumberFieldType intField = new NumberFieldMapper.NumberFieldType("my_int", NumberFieldMapper.NumberType.INTEGER);
        assigner.assign(intField);

        assertTrue("Capability map should be empty when no formats configured", intField.getCapabilityMap().isEmpty());
    }

    public void testAssignWithNullFormatsProducesEmptyMap() {
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(null);

        NumberFieldMapper.NumberFieldType intField = new NumberFieldMapper.NumberFieldType("my_int", NumberFieldMapper.NumberType.INTEGER);
        assigner.assign(intField);

        assertTrue("Capability map should be empty when formats is null", intField.getCapabilityMap().isEmpty());
    }

    public void testAssignThrowsWhenCapabilityUncovered() {
        // Only parquet, which doesn't support FULL_TEXT_SEARCH for keyword
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(List.of(PARQUET_FORMAT));

        KeywordFieldMapper.KeywordFieldType keywordField = new KeywordFieldMapper.KeywordFieldType("my_keyword");

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> assigner.assign(keywordField));
        assertTrue(e.getMessage().contains("cannot collectively cover them"));
        assertTrue(e.getMessage().contains("FULL_TEXT_SEARCH"));
    }

    public void testPriorityOrderMatters() {
        // Lucene first, then Parquet — Lucene should claim what it can first
        FieldCapabilityAssigner luceneFirst = new FieldCapabilityAssigner(List.of(LUCENE_FORMAT, PARQUET_FORMAT));

        NumberFieldMapper.NumberFieldType intField = new NumberFieldMapper.NumberFieldType("my_int", NumberFieldMapper.NumberType.INTEGER);
        luceneFirst.assign(intField);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = intField.getCapabilityMap();
        // Lucene is first and supports both POINT_RANGE and COLUMNAR_STORAGE (via FULL_TEXT_SEARCH mapped for search)
        // Actually Lucene declares STORED_FIELDS and FULL_TEXT_SEARCH for integer
        // But the integer field requests POINT_RANGE (searchCapability) and COLUMNAR_STORAGE (hasDocValues)
        // Lucene doesn't support POINT_RANGE or COLUMNAR_STORAGE for integer, so Parquet gets them
        assertTrue(
            "Parquet should still own COLUMNAR_STORAGE since Lucene doesn't declare it for integer",
            capMap.containsKey(PARQUET_FORMAT)
        );
    }

    public void testSeqNoLikeFieldAssignment() {
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(List.of(PARQUET_FORMAT, LUCENE_FORMAT));

        // _seq_no is a long with POINT_RANGE + COLUMNAR_STORAGE. Use NumberFieldType(long) to simulate.
        NumberFieldMapper.NumberFieldType seqNoField = new NumberFieldMapper.NumberFieldType("_seq_no", NumberFieldMapper.NumberType.LONG);
        assigner.assign(seqNoField);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = seqNoField.getCapabilityMap();
        assertFalse("Capability map should not be empty for _seq_no", capMap.isEmpty());
        assertTrue("Parquet should own capabilities for _seq_no", capMap.containsKey(PARQUET_FORMAT));
        assertTrue(
            "Parquet should own COLUMNAR_STORAGE for _seq_no",
            capMap.get(PARQUET_FORMAT).contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
        );
        assertTrue(
            "Parquet should own POINT_RANGE for _seq_no",
            capMap.get(PARQUET_FORMAT).contains(FieldTypeCapabilities.Capability.POINT_RANGE)
        );
    }

    public void testFieldWithNoRequestedCapabilitiesGetsEmptyMap() {
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(List.of(PARQUET_FORMAT, LUCENE_FORMAT));

        // A field that is not searchable, not stored, and has no doc values
        NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            "no_caps_field",
            NumberFieldMapper.NumberType.INTEGER,
            false,  // isSearchable
            false,  // isStored
            false,  // hasDocValues
            false,  // skiplist
            true,   // coerce
            null,   // nullValue
            Map.of()
        );
        assigner.assign(fieldType);

        assertTrue("Field with no requested capabilities should have empty map", fieldType.getCapabilityMap().isEmpty());
    }

    public void testDateFieldUsesPointRange() {
        FieldCapabilityAssigner assigner = new FieldCapabilityAssigner(List.of(PARQUET_FORMAT, LUCENE_FORMAT));

        DateFieldMapper.DateFieldType dateField = new DateFieldMapper.DateFieldType("my_date");
        assigner.assign(dateField);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = dateField.getCapabilityMap();
        assertFalse("Date field capability map should not be empty", capMap.isEmpty());
        // Date field requests POINT_RANGE (search), COLUMNAR_STORAGE (docValues)
        assertTrue(
            "Parquet should own POINT_RANGE for date",
            capMap.get(PARQUET_FORMAT).contains(FieldTypeCapabilities.Capability.POINT_RANGE)
        );
        assertTrue(
            "Parquet should own COLUMNAR_STORAGE for date",
            capMap.get(PARQUET_FORMAT).contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
        );
    }

}
