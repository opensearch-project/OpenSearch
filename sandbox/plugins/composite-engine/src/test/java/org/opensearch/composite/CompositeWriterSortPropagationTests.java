/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
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
        assertTrue("FlushInput should have sort permutation", secondaryWriter.lastFlushInput.hasSortPermutation());
        assertArrayEquals(permutation[0], secondaryWriter.lastFlushInput.sortPermutation()[0]);
        assertArrayEquals(permutation[1], secondaryWriter.lastFlushInput.sortPermutation()[1]);

        // The composite FileInfos should also carry the sort permutation
        assertNotNull(result.sortPermutation());
        assertArrayEquals(permutation[0], result.sortPermutation()[0]);

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
        assertFalse("FlushInput should not have sort permutation", secondaryWriter.lastFlushInput.hasSortPermutation());

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
        final long[][] sortPermutationToReturn;
        FlushInput lastFlushInput;

        RecordingWriter(DataFormat format, long[][] sortPermutationToReturn) {
            this.format = format;
            this.sortPermutationToReturn = sortPermutationToReturn;
        }

        @Override
        public WriteResult addDoc(DocumentInput<?> d) {
            return new WriteResult.Success(1, 1, 1);
        }

        @Override
        public FileInfos flush(FlushInput flushInput) {
            this.lastFlushInput = flushInput;
            FileInfos.Builder builder = FileInfos.builder();
            if (sortPermutationToReturn != null) {
                builder.sortPermutation(sortPermutationToReturn);
            }
            return builder.build();
        }

        @Override
        public void sync() {}

        @Override
        public void close() {}

        @Override
        public long generation() { return 0; }

        @Override
        public void lock() {}

        @Override
        public boolean tryLock() { return true; }

        @Override
        public void unlock() {}
    }
}
