/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.lucene.search.Query;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Map;

public class ParquetDocumentInputTests extends ParquetBaseTests {

    private static final DataFormat PARQUET_FORMAT = new ParquetDataFormat();

    public void testAddFieldAndGetFinalInput() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(ft, PARQUET_FORMAT);
        input.addField(ft, 25);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        populateMetadataFields(input);
        List<FieldValuePair> result = input.getFinalInput();
        assertEquals(5, result.size());
        assertSame(ft, result.getFirst().getFieldType());
        assertEquals(25, result.getFirst().getValue());
    }

    public void testMultipleFields() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        MappedFieldType ft1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType ft2 = new KeywordFieldMapper.KeywordFieldType("b");
        assignTestCapabilities(ft1, PARQUET_FORMAT);
        assignTestCapabilities(ft2, PARQUET_FORMAT);
        input.addField(ft1, 1);
        input.addField(ft2, "val");
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        populateMetadataFields(input);
        assertEquals(6, input.getFinalInput().size());
    }

    public void testEmptyInput() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        assertEquals(4, input.getFinalInput().size());
    }

    public void testSetRowId() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 42L);
        assertEquals(42L, input.getRowId());
    }

    public void testCloseClearsState() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(ft, PARQUET_FORMAT);
        input.addField(ft, 25);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        assertEquals(5, input.getFinalInput().size());

        input.close();
        assertTrue(input.getFinalInput().isEmpty());
    }

    public void testRejectsDuplicateFieldInSingleDocument() throws Exception {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(valField, PARQUET_FORMAT);

        input.addField(valField, 10);
        expectThrows(MapperParsingException.class, () -> input.addField(valField, 20));
    }

    /** Two values for the same leaf within ONE nested element must be rejected, like the top-level dedup. */
    public void testRejectsDuplicateLeafWithinSameNestedElement() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        KeywordFieldMapper.KeywordFieldType tag = new KeywordFieldMapper.KeywordFieldType("comments.tags");
        assignTestCapabilities(tag, PARQUET_FORMAT);

        input.addField(nestedPathMarker(), "comments");
        input.addField(tag, "a");
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> input.addField(tag, "b"));
        assertTrue(e.getMessage().contains("comments.tags"));
    }

    /** The SAME leaf name repeating across DIFFERENT elements is fine — each element gets its own value. */
    public void testSameLeafNameAcrossDifferentNestedElementsIsFine() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        KeywordFieldMapper.KeywordFieldType author = new KeywordFieldMapper.KeywordFieldType("comments.author");
        assignTestCapabilities(author, PARQUET_FORMAT);

        input.addField(nestedPathMarker(), "comments");
        input.addField(author, "alice");
        input.addField(nestedPathMarker(), "comments");
        input.addField(author, "bob");
        // Nothing moves the last-open element into getNestedChildren() without a later signal or an
        // explicit flush; getFinalInput() would also flush but its id/seqno/rowId assertions don't apply
        // here, so flush directly instead.
        flushOpenElements(input);

        assertEquals(2, input.getNestedChildren().size());
        assertEquals("alice", input.getNestedChildren().get(0).fields.get(0).value);
        assertEquals("bob", input.getNestedChildren().get(1).fields.get(0).value);
    }

    private static void flushOpenElements(ParquetDocumentInput input) {
        try {
            java.lang.reflect.Method method = ParquetDocumentInput.class.getDeclaredMethod("flushOpenElements");
            method.setAccessible(true);
            method.invoke(input);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A field type reporting {@code typeName() == NestedPathFieldMapper.NAME} — what
     * {@code ParquetDocumentInput} keys the nested-element marker recognition on (the real
     * {@code NestedPathFieldMapper.NestedPathFieldType} has no public constructor).
     */
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
}
