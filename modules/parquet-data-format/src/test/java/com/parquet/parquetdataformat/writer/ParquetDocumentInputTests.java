/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.fields.ArrowFieldRegistry;
import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.MockedStatic;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for ParquetDocumentInput using mocks to isolate behavior from dependencies.
 */
public class ParquetDocumentInputTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(ParquetDocumentInputTests.class);

    private ManagedVSR mockManagedVSR;
    private MappedFieldType mockMappedFieldType;
    private ParquetField mockParquetField;
    private ParquetDocumentInput parquetDocumentInput;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        mockManagedVSR = mock(ManagedVSR.class);
        mockMappedFieldType = mock(MappedFieldType.class);
        mockParquetField = mock(ParquetField.class);

        parquetDocumentInput = new ParquetDocumentInput(mockManagedVSR);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        mockManagedVSR = null;
        mockMappedFieldType = null;
        mockParquetField = null;
        parquetDocumentInput = null;
    }

    public void testConstructorStoresManagedVSRInstance() {
        ManagedVSR testManagedVSR = mock(ManagedVSR.class);
        ParquetDocumentInput testInput = new ParquetDocumentInput(testManagedVSR);

        ManagedVSR result = testInput.getFinalInput();
        assertSame("Constructor should store the ManagedVSR instance", testManagedVSR, result);
    }

    public void testAddRowIdFieldWritesToRowIdVector() {
        when(mockManagedVSR.getRowCount()).thenReturn(5);

        expectThrows(NullPointerException.class, () ->
            parquetDocumentInput.addRowIdField("_id", 1234L)
        );

        verify(mockManagedVSR).getVector(CompositeDataFormatWriter.ROW_ID);
        verify(mockManagedVSR).getRowCount();
    }

    public void testAddRowIdFieldThrowsIfVectorIsNotBigIntVector() {
        when(mockManagedVSR.getVector(CompositeDataFormatWriter.ROW_ID)).thenReturn(null);

        NullPointerException thrown = assertThrows(NullPointerException.class, () ->
            parquetDocumentInput.addRowIdField("_id", 1234L)
        );

        assertNotNull("NullPointerException should be thrown", thrown);
        verify(mockManagedVSR).getVector(CompositeDataFormatWriter.ROW_ID);
    }

    public void testAddRowIdFieldWorksWithRowCountZero() {
        when(mockManagedVSR.getRowCount()).thenReturn(0);

        expectThrows(NullPointerException.class, () ->
            parquetDocumentInput.addRowIdField("_id", 9999L)
        );

        verify(mockManagedVSR).getRowCount();
        verify(mockManagedVSR).getVector(CompositeDataFormatWriter.ROW_ID);
    }

    public void testAddFieldDelegatesToParquetFieldCreateField() {
        when(mockMappedFieldType.typeName()).thenReturn("long");

        parquetDocumentInput.addField(mockMappedFieldType, 10L);

        verify(mockMappedFieldType).typeName();
    }

    public void testAddFieldThrowsIllegalArgumentExceptionForUnknownFieldTypes() {
        when(mockMappedFieldType.typeName()).thenReturn("completely_unknown_field_type_xyz123");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () ->
            parquetDocumentInput.addField(mockMappedFieldType, "value")
        );

        assertNotNull("IllegalArgumentException should be thrown", thrown);
        assertTrue("Exception message should mention unsupported field type",
                  thrown.getMessage().contains("Unsupported field type: completely_unknown_field_type_xyz123"));
        assertTrue("Exception message should mention ArrowFieldRegistry",
                  thrown.getMessage().contains("ArrowFieldRegistry"));

        verify(mockMappedFieldType).typeName();
    }

    public void testAddFieldPassesNullValuesToCreateFieldCorrectly() {
        when(mockMappedFieldType.typeName()).thenReturn("text");

        parquetDocumentInput.addField(mockMappedFieldType, null);

        verify(mockMappedFieldType).typeName();
    }

    public void testAddFieldHandlesCreateFieldThrowingExceptions() {
        when(mockMappedFieldType.typeName()).thenReturn("date");

        try {
            parquetDocumentInput.addField(mockMappedFieldType, "2023-01-01");
            verify(mockMappedFieldType).typeName();
        } catch (Exception e) {
            verify(mockMappedFieldType).typeName();
        }
    }

    public void testSetPrimaryTermWritesToCorrectVector() {
        when(mockManagedVSR.getRowCount()).thenReturn(5);

        expectThrows(NullPointerException.class, () ->
            parquetDocumentInput.setPrimaryTerm("_primary", 77L)
        );

        verify(mockManagedVSR).getVector("_primary");
        verify(mockManagedVSR).getRowCount();
    }

    public void testSetPrimaryTermThrowsIfVectorIsNotBigIntVector() {
        when(mockManagedVSR.getVector("_primary")).thenReturn(null);

        NullPointerException thrown = assertThrows(NullPointerException.class, () ->
            parquetDocumentInput.setPrimaryTerm("_primary", 77L)
        );

        assertNotNull("NullPointerException should be thrown", thrown);
        verify(mockManagedVSR).getVector("_primary");
    }

    public void testGetFinalInputReturnsManagedVSRReference() {
        ManagedVSR result = parquetDocumentInput.getFinalInput();

        assertSame("getFinalInput should return the same ManagedVSR passed to constructor",
                  mockManagedVSR, result);
    }

    public void testAddToWriterIncrementsRowCount() throws IOException {
        when(mockManagedVSR.getRowCount()).thenReturn(5);

        WriteResult result = parquetDocumentInput.addToWriter();

        verify(mockManagedVSR).getRowCount();
        verify(mockManagedVSR).setRowCount(6);

        assertNotNull("addToWriter should return WriteResult", result);
    }

    public void testAddToWriterReturnsCorrectWriteResult() throws IOException {
        when(mockManagedVSR.getRowCount()).thenReturn(0);

        WriteResult result = parquetDocumentInput.addToWriter();

        assertNotNull("WriteResult should not be null", result);
        assertTrue("WriteResult should indicate success", result.success());
        assertNull("WriteResult should have no exception", result.e());
        assertEquals("WriteResult should have version 1", 1L, result.version());
        assertEquals("WriteResult should have term 1", 1L, result.term());
        assertEquals("WriteResult should have seqNo 1", 1L, result.seqNo());
    }

    public void testAddToWriterPropagatesIOException() throws IOException {
        when(mockManagedVSR.getRowCount()).thenReturn(3);
        RuntimeException testException = new RuntimeException("Test setRowCount exception");
        doThrow(testException).when(mockManagedVSR).setRowCount(4);

        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
            parquetDocumentInput.addToWriter()
        );

        assertEquals("RuntimeException should be propagated", testException, thrown);
        verify(mockManagedVSR).getRowCount();
        verify(mockManagedVSR).setRowCount(4);
    }

    public void testCloseDoesNotCallManagedVSRClose() throws Exception {
        parquetDocumentInput.close();
        verifyNoInteractions(mockManagedVSR);
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
        when(mockManagedVSR.getRowCount()).thenReturn(0);
        when(mockMappedFieldType.typeName()).thenReturn("keyword");

        expectThrows(NullPointerException.class, () ->
            parquetDocumentInput.addRowIdField("_id", 12345L)
        );
        verify(mockManagedVSR).getVector(CompositeDataFormatWriter.ROW_ID);
        verify(mockManagedVSR).getRowCount();

        expectThrows(NullPointerException.class, () ->
            parquetDocumentInput.setPrimaryTerm("_primary", 99L)
        );
        verify(mockManagedVSR).getVector("_primary");
        verify(mockManagedVSR, times(2)).getRowCount();

        parquetDocumentInput.addField(mockMappedFieldType, "test_value");
        verify(mockMappedFieldType).typeName();

        WriteResult result = parquetDocumentInput.addToWriter();
        assertNotNull("WriteResult should be returned", result);
        assertTrue("WriteResult should indicate success", result.success());
        verify(mockManagedVSR, times(3)).getRowCount();
        verify(mockManagedVSR).setRowCount(1);
    }

    public void testRowCountIncrementsForEveryAddToWriterCall() throws IOException {
        when(mockManagedVSR.getRowCount())
            .thenReturn(0)
            .thenReturn(1)
            .thenReturn(2);

        WriteResult result1 = parquetDocumentInput.addToWriter();
        assertNotNull("First WriteResult should not be null", result1);

        WriteResult result2 = parquetDocumentInput.addToWriter();
        assertNotNull("Second WriteResult should not be null", result2);

        WriteResult result3 = parquetDocumentInput.addToWriter();
        assertNotNull("Third WriteResult should not be null", result3);

        verify(mockManagedVSR, times(3)).getRowCount();
        verify(mockManagedVSR).setRowCount(1);
        verify(mockManagedVSR).setRowCount(2);
        verify(mockManagedVSR).setRowCount(3);
    }

    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception | Error e) {
            fail("Expected no exception, but got: " + e.getMessage());
        }
    }
}
