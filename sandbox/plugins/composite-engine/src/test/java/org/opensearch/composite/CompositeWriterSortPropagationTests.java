/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that CompositeWriter.flush() correctly propagates the sort permutation
 * from the primary writer's FileInfos to the secondary writers via FlushInput.
 */
public class CompositeWriterSortPropagationTests extends OpenSearchTestCase {

    /**
     * When the primary writer returns a sort permutation in FileInfos,
     * the secondary writer should receive it via FlushInput.
     */
    public void testSortPermutationPropagatedToSecondary() throws IOException {
        long[][] permutation = { { 0, 1, 2 }, { 2, 0, 1 } };

        // Primary writer that returns FileInfos with a sort permutation
        DataFormat primaryFormat = mockFormat("parquet");
        RecordingWriter primaryWriter = new RecordingWriter(primaryFormat, permutation);

        // Secondary writer that records the FlushInput it receives
        DataFormat secondaryFormat = mockFormat("lucene");
        RecordingWriter secondaryWriter = new RecordingWriter(secondaryFormat, null);

        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngineWithWriters(
            primaryFormat, primaryWriter, secondaryFormat, secondaryWriter
        );

        CompositeWriter compositeWriter = new CompositeWriter(engine, 0);
        FileInfos result = compositeWriter.flush(FlushInput.EMPTY);

        // The secondary writer should have received the sort permutation
        assertNotNull("Secondary writer should have received FlushInput", secondaryWriter.lastFlushInput);
        assertTrue("FlushInput should have row ID mapping", secondaryWriter.lastFlushInput.hasRowIdMapping());
        RowIdMapping receivedMapping = secondaryWriter.lastFlushInput.rowIdMapping();
        assertNotNull(receivedMapping);
        assertEquals(3, receivedMapping.size());
        // permutation is {0,1,2} -> {2,0,1}: old pos 0 maps to new pos 2
        assertEquals(2L, receivedMapping.getNewRowId(0, RowIdMapping.SINGLE_GEN));
        assertEquals(0L, receivedMapping.getNewRowId(1, RowIdMapping.SINGLE_GEN));
        assertEquals(1L, receivedMapping.getNewRowId(2, RowIdMapping.SINGLE_GEN));

        // The composite FileInfos should also carry the sort permutation
        assertNotNull(result.rowIdMapping());

        compositeWriter.close();
    }

    /**
     * When the primary writer returns no sort permutation, the secondary
     * writer should receive FlushInput.EMPTY.
     */
    public void testNoSortPermutationPassesEmptyFlushInput() throws IOException {
        DataFormat primaryFormat = mockFormat("parquet");
        RecordingWriter primaryWriter = new RecordingWriter(primaryFormat, null);

        DataFormat secondaryFormat = mockFormat("lucene");
        RecordingWriter secondaryWriter = new RecordingWriter(secondaryFormat, null);

        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngineWithWriters(
            primaryFormat, primaryWriter, secondaryFormat, secondaryWriter
        );

        CompositeWriter compositeWriter = new CompositeWriter(engine, 0);
        compositeWriter.flush(FlushInput.EMPTY);

        assertNotNull(secondaryWriter.lastFlushInput);
        assertFalse("FlushInput should not have row ID mapping", secondaryWriter.lastFlushInput.hasRowIdMapping());

        compositeWriter.close();
    }

    /**
     * The primary writer should always receive the original FlushInput passed
     * to CompositeWriter.flush(), not a modified one.
     */
    public void testPrimaryReceivesOriginalFlushInput() throws IOException {
        DataFormat primaryFormat = mockFormat("parquet");
        RecordingWriter primaryWriter = new RecordingWriter(primaryFormat, null);

        DataFormat secondaryFormat = mockFormat("lucene");
        RecordingWriter secondaryWriter = new RecordingWriter(secondaryFormat, null);

        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngineWithWriters(
            primaryFormat, primaryWriter, secondaryFormat, secondaryWriter
        );

        CompositeWriter compositeWriter = new CompositeWriter(engine, 0);
        compositeWriter.flush(FlushInput.EMPTY);

        assertSame("Primary should receive the original FlushInput", FlushInput.EMPTY, primaryWriter.lastFlushInput);

        compositeWriter.close();
    }

    private DataFormat mockFormat(String name) {
        DataFormat format = mock(DataFormat.class);
        when(format.name()).thenReturn(name);
        when(format.priority()).thenReturn(1L);
        return format;
    }

    /**
     * A Writer stub that records the FlushInput it receives and optionally
     * returns a sort permutation in its FileInfos.
     */
    static class RecordingWriter implements Writer<DocumentInput<?>> {
        final DataFormat format;
        final RowIdMapping rowIdMappingToReturn;
        FlushInput lastFlushInput;

        RecordingWriter(DataFormat format, long[][] rawPermutation) {
            this.format = format;
            if (rawPermutation != null && rawPermutation.length == 2 && rawPermutation[0].length > 0) {
                int numDocs = rawPermutation[0].length;
                long[] oldToNew = new long[numDocs];
                for (int i = 0; i < numDocs; i++) {
                    oldToNew[i] = i;
                }
                for (int i = 0; i < rawPermutation[0].length; i++) {
                    oldToNew[(int) rawPermutation[0][i]] = rawPermutation[1][i];
                }
                this.rowIdMappingToReturn = new PackedRowIdMapping(oldToNew, true);
            } else {
                this.rowIdMappingToReturn = null;
            }
        }

        @Override
        public WriteResult addDoc(DocumentInput<?> d) {
            return new WriteResult.Success(1, 1, 1);
        }

        @Override
        public FileInfos flush(FlushInput flushInput) {
            this.lastFlushInput = flushInput;
            FileInfos.Builder builder = FileInfos.builder();
            if (rowIdMappingToReturn != null) {
                builder.rowIdMapping(rowIdMappingToReturn);
            }
            return builder.build();
        }

        @Override
        public void sync() {}

        @Override
        public void close() {}

        @Override
        public long generation() { return 0; }
    }
}
