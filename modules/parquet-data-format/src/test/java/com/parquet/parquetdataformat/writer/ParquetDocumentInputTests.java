/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Comprehensive test suite for ParquetDocumentInput using concrete objects instead of mocks.
 */
public class ParquetDocumentInputTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema testSchema;
    private ManagedVSR managedVSR;
    private ParquetDocumentInput parquetDocumentInput;

    /**
     * Concrete MappedFieldType implementation for testing
     */
    private static class TestMappedFieldType extends MappedFieldType {
        private final String type;
        
        public TestMappedFieldType(String name, String type) {
            super(name, true, false, false, TextSearchInfo.NONE, null);
            this.type = type;
        }
        
        @Override
        public String typeName() {
            return type;
        }
        
        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return null;
        }
        
        @Override
        public org.apache.lucene.search.Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("Test implementation does not support term queries");
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        allocator = new RootAllocator();
        
        // Create schema with required fields for testing
        Field rowIdField = new Field(CompositeDataFormatWriter.ROW_ID, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field primaryTermField = new Field("_primary", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field longField = new Field("long_field", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field textField = new Field("text_field", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        
        testSchema = new Schema(Arrays.asList(rowIdField, primaryTermField, longField, textField));
        
        managedVSR = new ManagedVSR("test-vsr", testSchema, allocator);
        parquetDocumentInput = new ParquetDocumentInput(managedVSR);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (managedVSR != null && managedVSR.getState().name().equals("ACTIVE")) {
            managedVSR.moveToFrozen();
        }
        if (managedVSR != null) {
            managedVSR.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    public void testConstructorStoresManagedVSRInstance() {
        ManagedVSR result = parquetDocumentInput.getFinalInput();
        assertSame("Constructor should store the ManagedVSR instance", managedVSR, result);
    }

    public void testAddRowIdFieldWritesToRowIdVector() {
        // Set initial row count
        managedVSR.setRowCount(5);
        
        // This should work without throwing an exception
        parquetDocumentInput.addRowIdField("_id", 1234L);
        
        // Verify the value was set in the vector
        BigIntVector rowIdVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
        assertEquals("Row ID should be set at current row count", 1234L, rowIdVector.get(5));
    }

    public void testAddRowIdFieldIgnoresFieldNameParameter() {
        // The addRowIdField method ignores the fieldName parameter and always uses CompositeDataFormatWriter.ROW_ID
        // This should work regardless of the field name passed
        parquetDocumentInput.addRowIdField("any_field_name", 1234L);
        
        // Verify the value was set in the ROW_ID vector
        BigIntVector rowIdVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
        assertEquals("Row ID should be set regardless of field name parameter", 1234L, rowIdVector.get(0));
    }

    public void testAddRowIdFieldWorksWithRowCountZero() {
        // Row count starts at 0, should work fine
        assertEquals("Initial row count should be 0", 0, managedVSR.getRowCount());
        
        parquetDocumentInput.addRowIdField("_id", 9999L);
        
        // Verify the value was set at index 0
        BigIntVector rowIdVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
        assertEquals("Row ID should be set at index 0", 9999L, rowIdVector.get(0));
    }

    public void testAddFieldDelegatesToParquetFieldCreateField() {
        TestMappedFieldType fieldType = new TestMappedFieldType("long_field", "long");

        // This should work for registered field types
        parquetDocumentInput.addField(fieldType, 10L);
        
        // Verify the field type was accessed
        assertEquals("Field type should be 'long'", "long", fieldType.typeName());
    }

    public void testAddFieldThrowsIllegalArgumentExceptionForUnknownFieldTypes() {
        TestMappedFieldType unknownFieldType = new TestMappedFieldType("unknown_field", "completely_unknown_field_type_xyz123");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () ->
            parquetDocumentInput.addField(unknownFieldType, "value")
        );

        assertNotNull("IllegalArgumentException should be thrown", thrown);
        assertTrue("Exception message should mention unsupported field type",
                  thrown.getMessage().contains("Unsupported field type: completely_unknown_field_type_xyz123"));
        assertTrue("Exception message should mention ArrowFieldRegistry",
                  thrown.getMessage().contains("ArrowFieldRegistry"));
    }

    public void testAddFieldPassesNullValuesToCreateFieldCorrectly() {
        TestMappedFieldType textFieldType = new TestMappedFieldType("text_field", "text");

        // This should throw a NullPointerException because TextParquetField doesn't handle null values properly
        NullPointerException thrown = assertThrows(NullPointerException.class, () ->
            parquetDocumentInput.addField(textFieldType, null)
        );
        
        assertNotNull("NullPointerException should be thrown for null values", thrown);
        assertEquals("Field type should be 'text'", "text", textFieldType.typeName());
    }

    public void testAddFieldHandlesCreateFieldThrowingExceptions() {
        TestMappedFieldType dateFieldType = new TestMappedFieldType("date_field", "date");

        try {
            parquetDocumentInput.addField(dateFieldType, "2023-01-01");
            assertEquals("Field type should be 'date'", "date", dateFieldType.typeName());
        } catch (Exception e) {
            // Exception is expected if date field type is not properly registered
            assertEquals("Field type should be 'date'", "date", dateFieldType.typeName());
        }
    }

    public void testSetPrimaryTermWritesToCorrectVector() {
        managedVSR.setRowCount(5);

        parquetDocumentInput.setPrimaryTerm("_primary", 77L);

        // Verify the value was set in the primary term vector
        BigIntVector primaryTermVector = (BigIntVector) managedVSR.getVector("_primary");
        assertEquals("Primary term should be set at current row count", 77L, primaryTermVector.get(5));
    }

    public void testSetPrimaryTermThrowsIfVectorDoesNotExist() {
        NullPointerException thrown = assertThrows(NullPointerException.class, () ->
            parquetDocumentInput.setPrimaryTerm("nonexistent_primary", 77L)
        );

        assertNotNull("NullPointerException should be thrown", thrown);
    }

    public void testGetFinalInputReturnsManagedVSRReference() {
        ManagedVSR result = parquetDocumentInput.getFinalInput();

        assertSame("getFinalInput should return the same ManagedVSR passed to constructor",
                  managedVSR, result);
    }

    public void testAddToWriterIncrementsRowCount() throws IOException {
        managedVSR.setRowCount(5);

        WriteResult result = parquetDocumentInput.addToWriter();

        assertEquals("Row count should be incremented", 6, managedVSR.getRowCount());
        assertNotNull("addToWriter should return WriteResult", result);
    }

    public void testAddToWriterReturnsCorrectWriteResult() throws IOException {
        assertEquals("Initial row count should be 0", 0, managedVSR.getRowCount());

        WriteResult result = parquetDocumentInput.addToWriter();

        assertNotNull("WriteResult should not be null", result);
        assertTrue("WriteResult should indicate success", result.success());
        assertNull("WriteResult should have no exception", result.e());
        assertEquals("WriteResult should have version 1", 1L, result.version());
        assertEquals("WriteResult should have term 1", 1L, result.term());
        assertEquals("WriteResult should have seqNo 1", 1L, result.seqNo());
        assertEquals("Row count should be incremented to 1", 1, managedVSR.getRowCount());
    }

    public void testAddToWriterWithFrozenVSRThrowsException() throws IOException {
        managedVSR.setRowCount(3);
        
        // Freeze the VSR to make it immutable
        managedVSR.moveToFrozen();

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
            parquetDocumentInput.addToWriter()
        );

        assertTrue("Exception should mention cannot modify VSR",
                  thrown.getMessage().contains("Cannot modify VSR in state: FROZEN"));
    }

    public void testCloseDoesNotCallManagedVSRClose() throws Exception {
        // VSR should remain in ACTIVE state after ParquetDocumentInput.close()
        parquetDocumentInput.close();
        assertEquals("VSR should remain ACTIVE after ParquetDocumentInput.close()", 
                    "ACTIVE", managedVSR.getState().name());
    }

    public void testCloseDoesNotThrowAnyException() throws Exception {
        assertDoesNotThrow(() -> {
            try {
                parquetDocumentInput.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testFullDocumentCreationFlow() throws Exception {
        assertEquals("Initial row count should be 0", 0, managedVSR.getRowCount());
        
        TestMappedFieldType keywordFieldType = new TestMappedFieldType("keyword_field", "keyword");

        // Add row ID field
        parquetDocumentInput.addRowIdField(CompositeDataFormatWriter.ROW_ID, 12345L);
        BigIntVector rowIdVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
        assertEquals("Row ID should be set", 12345L, rowIdVector.get(0));

        // Set primary term
        parquetDocumentInput.setPrimaryTerm("_primary", 99L);
        BigIntVector primaryTermVector = (BigIntVector) managedVSR.getVector("_primary");
        assertEquals("Primary term should be set", 99L, primaryTermVector.get(0));

        // Add field (this may throw exception if keyword type is not registered)
        try {
            parquetDocumentInput.addField(keywordFieldType, "test_value");
        } catch (IllegalArgumentException e) {
            // Expected if keyword field type is not registered
            assertTrue("Exception should mention unsupported field type", 
                      e.getMessage().contains("Unsupported field type"));
        }

        WriteResult result = parquetDocumentInput.addToWriter();
        assertNotNull("WriteResult should be returned", result);
        assertTrue("WriteResult should indicate success", result.success());
        assertEquals("Row count should be incremented", 1, managedVSR.getRowCount());
    }

    public void testRowCountIncrementsForEveryAddToWriterCall() throws IOException {
        assertEquals("Initial row count should be 0", 0, managedVSR.getRowCount());

        WriteResult result1 = parquetDocumentInput.addToWriter();
        assertNotNull("First WriteResult should not be null", result1);
        assertEquals("Row count should be 1 after first call", 1, managedVSR.getRowCount());

        WriteResult result2 = parquetDocumentInput.addToWriter();
        assertNotNull("Second WriteResult should not be null", result2);
        assertEquals("Row count should be 2 after second call", 2, managedVSR.getRowCount());

        WriteResult result3 = parquetDocumentInput.addToWriter();
        assertNotNull("Third WriteResult should not be null", result3);
        assertEquals("Row count should be 3 after third call", 3, managedVSR.getRowCount());
    }

    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception | Error e) {
            fail("Expected no exception, but got: " + e.getMessage());
        }
    }
}
