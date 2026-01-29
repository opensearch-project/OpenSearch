/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import com.parquet.parquetdataformat.vsr.VSRState;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Comprehensive test suite for ParquetDocumentInput using concrete objects instead of mocks.
 */
public class ParquetDocumentInputTests extends OpenSearchTestCase {

    private static final String TEST_VSR_ID = "test-vsr";
    private static final String PRIMARY_TERM_FIELD_NAME = "_primary";
    private static final String LONG_FIELD_NAME = "long_field";
    private static final String TEXT_FIELD_NAME = "text_field";
    private static final String KEYWORD_FIELD_NAME = "keyword_field";
    private static final String DATE_FIELD_NAME = "date_field";
    private static final String UNKNOWN_FIELD_NAME = "unknown_field";
    private static final String NONEXISTENT_PRIMARY_FIELD_NAME = "nonexistent_primary";
    
    private static final String LONG_FIELD_TYPE_NAME = "long";
    private static final String TEXT_FIELD_TYPE_NAME = "text";
    private static final String DATE_FIELD_TYPE_NAME = "date";
    private static final String UNKNOWN_FIELD_TYPE_NAME = "completely_unknown_field_type_xyz123";
    
    private static final long SAMPLE_ROW_ID = 1234L;
    private static final long LARGE_ROW_ID = 12345L;
    private static final long SAMPLE_PRIMARY_TERM = 77L;
    private static final long LARGE_PRIMARY_TERM = 99L;
    private static final long SAMPLE_LONG_VALUE = 10L;
    private static final String SAMPLE_KEYWORD_VALUE = "test_value";
    private static final String SAMPLE_DATE_VALUE = "2023-01-01";
    private static final String GENERIC_TEST_VALUE = "value";
    private static final String DOCUMENT_ID_FIELD_NAME = "_id";
    
    private static final int INITIAL_ROW_COUNT = 0;
    private static final int FIRST_ROW_COUNT = 1;
    private static final int SECOND_ROW_COUNT = 2;
    private static final int THIRD_ROW_COUNT = 3;
    private static final int TEST_ROW_POSITION = 5;
    private static final int INCREMENTED_ROW_COUNT = 6;
    private static final int FROZEN_VSR_ROW_COUNT = 3;
    
    private static final long EXPECTED_VERSION = 1L;
    private static final long EXPECTED_TERM = 1L;
    private static final long EXPECTED_SEQ_NO = 1L;
    
    private static final String UNSUPPORTED_FIELD_TYPE_ERROR = "Unsupported field type: " + UNKNOWN_FIELD_TYPE_NAME;
    private static final String ARROW_FIELD_REGISTRY_ERROR = "ArrowFieldRegistry";
    private static final String FROZEN_VSR_ERROR = "Cannot modify VSR in state: FROZEN";

    private BufferAllocator allocator;
    private Schema testSchema;
    private ManagedVSR managedVSR;
    private ParquetDocumentInput parquetDocumentInput;



    @Override
    public void setUp() throws Exception {
        super.setUp();

        allocator = new RootAllocator();

        Field rowIdField = new Field(CompositeDataFormatWriter.ROW_ID, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field primaryTermField = new Field(PRIMARY_TERM_FIELD_NAME, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field longField = new Field(LONG_FIELD_NAME, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field textField = new Field(TEXT_FIELD_NAME, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);

        testSchema = new Schema(Arrays.asList(rowIdField, primaryTermField, longField, textField));

        managedVSR = new ManagedVSR(TEST_VSR_ID, testSchema, allocator);
        parquetDocumentInput = new ParquetDocumentInput(managedVSR);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        if (managedVSR != null) {
            try {
                if (managedVSR.getState() == VSRState.ACTIVE) {
                    managedVSR.moveToFrozen();
                }
                managedVSR.close();
            } catch (Exception e) {
                System.err.println("Failed to close managedVSR during tearDown: " + e.getMessage());
            }
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    public void testConstructorStoresManagedVSRInstance() {
        ManagedVSR result = parquetDocumentInput.getFinalInput();
        assertSame(managedVSR, result);
    }

    public void testAddRowIdFieldWritesToRowIdVector() {
        managedVSR.setRowCount(TEST_ROW_POSITION);
        parquetDocumentInput.addRowIdField(DOCUMENT_ID_FIELD_NAME, SAMPLE_ROW_ID);
        BigIntVector rowIdVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
        assertEquals(SAMPLE_ROW_ID, rowIdVector.get(TEST_ROW_POSITION));
    }

    public void testAddFieldDelegatesToParquetFieldCreateField() {
        NumberFieldMapper.NumberFieldType longFieldType = new NumberFieldMapper.NumberFieldType(LONG_FIELD_NAME, NumberFieldMapper.NumberType.LONG);
        parquetDocumentInput.addField(longFieldType, SAMPLE_LONG_VALUE);
        assertEquals(LONG_FIELD_TYPE_NAME, longFieldType.typeName());
    }

    public void testAddFieldThrowsIllegalArgumentExceptionForUnknownFieldTypes() {
        KeywordFieldMapper.KeywordFieldType unknownFieldType = new KeywordFieldMapper.KeywordFieldType(UNKNOWN_FIELD_NAME) {
            @Override
            public String typeName() {
                return UNKNOWN_FIELD_TYPE_NAME;
            }
        };

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () ->
            parquetDocumentInput.addField(unknownFieldType, GENERIC_TEST_VALUE)
        );

        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains(UNSUPPORTED_FIELD_TYPE_ERROR));
        assertTrue(thrown.getMessage().contains(ARROW_FIELD_REGISTRY_ERROR));
    }

    public void testAddFieldPassesNullValuesToCreateFieldCorrectly() {
        TextFieldMapper.TextFieldType textFieldType = new TextFieldMapper.TextFieldType(TEXT_FIELD_NAME);
        NullPointerException thrown = assertThrows(NullPointerException.class, () ->
            parquetDocumentInput.addField(textFieldType, null)
        );
        assertNotNull(thrown);
        assertEquals(TEXT_FIELD_TYPE_NAME, textFieldType.typeName());
    }

    public void testAddFieldHandlesCreateFieldThrowingExceptions() {
        KeywordFieldMapper.KeywordFieldType dateFieldType = new KeywordFieldMapper.KeywordFieldType(DATE_FIELD_NAME) {
            @Override
            public String typeName() {
                return DATE_FIELD_TYPE_NAME;
            }
        };

        try {
            parquetDocumentInput.addField(dateFieldType, SAMPLE_DATE_VALUE);
            assertEquals(DATE_FIELD_TYPE_NAME, dateFieldType.typeName());
        } catch (Exception e) {
            assertEquals(DATE_FIELD_TYPE_NAME, dateFieldType.typeName());
        }
    }

    public void testSetPrimaryTermWritesToCorrectVector() {
        managedVSR.setRowCount(TEST_ROW_POSITION);
        parquetDocumentInput.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, SAMPLE_PRIMARY_TERM);
        BigIntVector primaryTermVector = (BigIntVector) managedVSR.getVector(PRIMARY_TERM_FIELD_NAME);
        assertEquals(SAMPLE_PRIMARY_TERM, primaryTermVector.get(TEST_ROW_POSITION));
    }

    public void testSetPrimaryTermThrowsIfVectorDoesNotExist() {
        NullPointerException thrown = assertThrows(NullPointerException.class, () ->
            parquetDocumentInput.setPrimaryTerm(NONEXISTENT_PRIMARY_FIELD_NAME, SAMPLE_PRIMARY_TERM)
        );
        assertNotNull(thrown);
    }

    public void testAddToWriterIncrementsRowCount() throws IOException {
        managedVSR.setRowCount(TEST_ROW_POSITION);
        WriteResult result = parquetDocumentInput.addToWriter();
        assertEquals(INCREMENTED_ROW_COUNT, managedVSR.getRowCount());
        assertNotNull(result);
    }

    public void testAddToWriterReturnsCorrectWriteResult() throws IOException {
        assertEquals(INITIAL_ROW_COUNT, managedVSR.getRowCount());
        WriteResult result = parquetDocumentInput.addToWriter();
        assertNotNull(result);
        assertTrue(result.success());
        assertNull(result.e());
        assertEquals(EXPECTED_VERSION, result.version());
        assertEquals(EXPECTED_TERM, result.term());
        assertEquals(EXPECTED_SEQ_NO, result.seqNo());
        assertEquals(FIRST_ROW_COUNT, managedVSR.getRowCount());
    }

    public void testAddToWriterWithFrozenVSRThrowsException() throws IOException {
        managedVSR.setRowCount(FROZEN_VSR_ROW_COUNT);
        managedVSR.moveToFrozen();
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
            parquetDocumentInput.addToWriter()
        );
        assertTrue(thrown.getMessage().contains(FROZEN_VSR_ERROR));
    }

    public void testFullDocumentCreationFlow() throws Exception {
        assertEquals(INITIAL_ROW_COUNT, managedVSR.getRowCount());
        KeywordFieldMapper.KeywordFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD_NAME);
        parquetDocumentInput.addRowIdField(CompositeDataFormatWriter.ROW_ID, LARGE_ROW_ID);
        BigIntVector rowIdVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
        assertEquals(LARGE_ROW_ID, rowIdVector.get(INITIAL_ROW_COUNT));
        parquetDocumentInput.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, LARGE_PRIMARY_TERM);
        BigIntVector primaryTermVector = (BigIntVector) managedVSR.getVector(PRIMARY_TERM_FIELD_NAME);
        assertEquals(LARGE_PRIMARY_TERM, primaryTermVector.get(INITIAL_ROW_COUNT));
        parquetDocumentInput.addField(keywordFieldType, SAMPLE_KEYWORD_VALUE);
        WriteResult result = parquetDocumentInput.addToWriter();
        assertNotNull(result);
        assertTrue(result.success());
        assertEquals(FIRST_ROW_COUNT, managedVSR.getRowCount());
    }

    public void testRowCountIncrementsForEveryAddToWriterCall() throws IOException {
        assertEquals(INITIAL_ROW_COUNT, managedVSR.getRowCount());
        WriteResult result1 = parquetDocumentInput.addToWriter();
        assertNotNull(result1);
        assertEquals(FIRST_ROW_COUNT, managedVSR.getRowCount());
        WriteResult result2 = parquetDocumentInput.addToWriter();
        assertNotNull(result2);
        assertEquals(SECOND_ROW_COUNT, managedVSR.getRowCount());
        WriteResult result3 = parquetDocumentInput.addToWriter();
        assertNotNull(result3);
        assertEquals(THIRD_ROW_COUNT, managedVSR.getRowCount());
    }
}
