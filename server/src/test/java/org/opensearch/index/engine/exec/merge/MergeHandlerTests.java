/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.engine.exec.coord.Segment;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class MergeHandlerTests extends OpenSearchTestCase {

    private MergeHandler mergeHandler;
    private CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;
    private Any compositeDataFormat;
    private DataFormat primaryDataFormat;
    private DataFormat secondaryDataFormat;
    private Merger primaryMerger;
    private Merger secondaryMerger;
    private IndexingExecutionEngine primaryEngine;
    private IndexingExecutionEngine secondaryEngine;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        ShardId shardId = new ShardId(new Index("test", "uuid"), 0);
        CompositeEngine compositeEngine = mock(CompositeEngine.class);
        compositeIndexingExecutionEngine = mock(CompositeIndexingExecutionEngine.class);
        compositeDataFormat = mock(Any.class);

        primaryDataFormat = mock(DataFormat.class);
        secondaryDataFormat = mock(DataFormat.class);
        primaryMerger = mock(Merger.class);
        secondaryMerger = mock(Merger.class);
        primaryEngine = mock(IndexingExecutionEngine.class);
        secondaryEngine = mock(IndexingExecutionEngine.class);

        when(primaryDataFormat.name()).thenReturn("primary");
        when(secondaryDataFormat.name()).thenReturn("secondary");
        when(compositeDataFormat.getPrimaryDataFormat()).thenReturn(primaryDataFormat);

        when(primaryEngine.getDataFormat()).thenReturn(primaryDataFormat);
        when(primaryEngine.getMerger()).thenReturn(primaryMerger);
        when(secondaryEngine.getDataFormat()).thenReturn(secondaryDataFormat);
        when(secondaryEngine.getMerger()).thenReturn(secondaryMerger);

        when(compositeIndexingExecutionEngine.getDelegates()).thenReturn(Arrays.asList(primaryEngine, secondaryEngine));
        when(compositeIndexingExecutionEngine.getNextWriterGeneration()).thenReturn(1L);

        mergeHandler = new TestMergeHandler(compositeEngine, compositeIndexingExecutionEngine, compositeDataFormat, shardId);
    }

    public void testDoMergeSuccess() {
        OneMerge oneMerge = createTestOneMerge();
        RowIdMapping rowIdMapping = mock(RowIdMapping.class);
        WriterFileSet primaryWriterFileSet = mock(WriterFileSet.class);
        WriterFileSet secondaryWriterFileSet = mock(WriterFileSet.class);

        MergeResult primaryMergeResult = mock(MergeResult.class);
        MergeResult secondaryMergeResult = mock(MergeResult.class);

        when(primaryMergeResult.getRowIdMapping()).thenReturn(rowIdMapping);
        when(primaryMergeResult.getMergedWriterFileSetForDataformat(primaryDataFormat)).thenReturn(primaryWriterFileSet);
        when(secondaryMergeResult.getMergedWriterFileSetForDataformat(secondaryDataFormat)).thenReturn(secondaryWriterFileSet);

        when(primaryMerger.merge(any(), anyLong())).thenReturn(primaryMergeResult);
        when(secondaryMerger.merge(any(), eq(rowIdMapping), anyLong())).thenReturn(secondaryMergeResult);

        MergeResult result = mergeHandler.doMerge(oneMerge);

        assertNotNull(result);
        assertEquals(rowIdMapping, result.getRowIdMapping());
        assertEquals(primaryWriterFileSet, result.getMergedWriterFileSetForDataformat(primaryDataFormat));
        assertEquals(secondaryWriterFileSet, result.getMergedWriterFileSetForDataformat(secondaryDataFormat));

        verify(primaryMerger, times(1)).merge(any(), eq(1L));
        verify(secondaryMerger, times(1)).merge(any(), eq(rowIdMapping), eq(1L));
    }

    public void testDoMergePrimaryFailure() {
        OneMerge oneMerge = createTestOneMerge();

        when(primaryMerger.merge(any(), anyLong())).thenThrow(new RuntimeException("Primary merge failed"));

        RuntimeException exception = expectThrows(RuntimeException.class, () -> mergeHandler.doMerge(oneMerge));
        assertEquals("Primary merge failed", exception.getMessage());

        verify(primaryMerger, times(1)).merge(any(), eq(1L));
        verify(secondaryMerger, times(0)).merge(any(), any(), anyLong());
    }

    public void testDoMergeSecondaryFailure() {
        OneMerge oneMerge = createTestOneMerge();
        RowIdMapping rowIdMapping = mock(RowIdMapping.class);
        WriterFileSet primaryWriterFileSet = mock(WriterFileSet.class);

        MergeResult primaryMergeResult = mock(MergeResult.class);
        when(primaryMergeResult.getRowIdMapping()).thenReturn(rowIdMapping);
        when(primaryMergeResult.getMergedWriterFileSetForDataformat(primaryDataFormat)).thenReturn(primaryWriterFileSet);

        when(primaryMerger.merge(any(), anyLong())).thenReturn(primaryMergeResult);
        when(secondaryMerger.merge(any(), eq(rowIdMapping), anyLong())).thenThrow(new RuntimeException("Secondary merge failed"));

        RuntimeException exception = expectThrows(RuntimeException.class, () -> mergeHandler.doMerge(oneMerge));
        assertEquals("Secondary merge failed", exception.getMessage());

        verify(primaryMerger, times(1)).merge(any(), eq(1L));
        verify(secondaryMerger, times(1)).merge(any(), eq(rowIdMapping), eq(1L));
    }

    private OneMerge createTestOneMerge() {
        Segment segment1 = mock(Segment.class);
        Segment segment2 = mock(Segment.class);

        WriterFileSet primaryFileSet1 = mock(WriterFileSet.class);
        WriterFileSet primaryFileSet2 = mock(WriterFileSet.class);
        WriterFileSet secondaryFileSet1 = mock(WriterFileSet.class);
        WriterFileSet secondaryFileSet2 = mock(WriterFileSet.class);

        Map<String, WriterFileSet> dfGroupedFiles1 = new HashMap<>();
        dfGroupedFiles1.put("primary", primaryFileSet1);
        dfGroupedFiles1.put("secondary", secondaryFileSet1);

        Map<String, WriterFileSet> dfGroupedFiles2 = new HashMap<>();
        dfGroupedFiles2.put("primary", primaryFileSet2);
        dfGroupedFiles2.put("secondary", secondaryFileSet2);

        when(segment1.getDFGroupedSearchableFiles()).thenReturn(dfGroupedFiles1);
        when(segment2.getDFGroupedSearchableFiles()).thenReturn(dfGroupedFiles2);

        return new OneMerge(Arrays.asList(segment1, segment2));
    }

    private static class TestMergeHandler extends MergeHandler {
        public TestMergeHandler(CompositeEngine compositeEngine, CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
                               Any dataFormats, ShardId shardId) {
            super(compositeEngine, compositeIndexingExecutionEngine, dataFormats, shardId);
        }

        @Override
        public java.util.Collection<OneMerge> findMerges() {
            return java.util.Collections.emptyList();
        }

        @Override
        public java.util.Collection<OneMerge> findForceMerges(int maxSegmentCount) {
            return java.util.Collections.emptyList();
        }
    }
}

