/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.engine.exec.coord.Segment;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class CompositeMergeHandlerTests extends OpenSearchTestCase {

    private CompositeMergeHandler compositeMergeHandler;
    private CompositeEngine compositeEngine;
    private CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;
    private Any compositeDataFormat;
    private IndexSettings indexSettings;
    private ShardId shardId;
    private CompositeMergePolicy mergePolicy;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        shardId = new ShardId(new Index("test", "uuid"), 0);
        compositeEngine = mock(CompositeEngine.class);
        compositeIndexingExecutionEngine = mock(CompositeIndexingExecutionEngine.class);
        compositeDataFormat = mock(Any.class);

        Settings settings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .build();

        indexSettings = new IndexSettings(
            org.opensearch.cluster.metadata.IndexMetadata.builder("test")
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            settings
        );

        when(compositeIndexingExecutionEngine.getDelegates()).thenReturn(Arrays.asList());

        compositeMergeHandler = new CompositeMergeHandler(
            compositeEngine,
            compositeIndexingExecutionEngine,
            compositeDataFormat,
            indexSettings,
            shardId
        );
    }

    public void testFindMerges() {
        Segment segment1 = mock(Segment.class);
        Segment segment2 = mock(Segment.class);
        List<Segment> segments = Arrays.asList(segment1, segment2);

        CompositeEngine.ReleasableRef<CatalogSnapshot> mockRef = mock(CompositeEngine.ReleasableRef.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockRef.getRef()).thenReturn(mockSnapshot);
        when(mockSnapshot.getSegments()).thenReturn(segments);
        when(compositeEngine.acquireSnapshot()).thenReturn(mockRef);

        Collection<OneMerge> result = compositeMergeHandler.findMerges();

        assertNotNull(result);
        verify(compositeEngine, times(1)).acquireSnapshot();
        verify(mockSnapshot, times(1)).getSegments();
    }

    public void testFindForceMerges() {
        Segment segment1 = mock(Segment.class);
        Segment segment2 = mock(Segment.class);
        List<Segment> segments = Arrays.asList(segment1, segment2);

        CompositeEngine.ReleasableRef<CatalogSnapshot> mockRef = mock(CompositeEngine.ReleasableRef.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockRef.getRef()).thenReturn(mockSnapshot);
        when(mockSnapshot.getSegments()).thenReturn(segments);
        when(compositeEngine.acquireSnapshot()).thenReturn(mockRef);

        Collection<OneMerge> result = compositeMergeHandler.findForceMerges(1);

        assertNotNull(result);
        verify(compositeEngine, times(1)).acquireSnapshot();
        verify(mockSnapshot, times(1)).getSegments();
    }

    public void testFindMergesException() {
        when(compositeEngine.acquireSnapshot()).thenThrow(new RuntimeException("Test exception"));

        RuntimeException exception = expectThrows(RuntimeException.class, () -> compositeMergeHandler.findMerges());
        assertTrue(exception.getMessage().contains("Test exception"));
    }

    public void testFindForceMergesException() {
        when(compositeEngine.acquireSnapshot()).thenThrow(new RuntimeException("Test exception"));

        RuntimeException exception = expectThrows(RuntimeException.class, () -> compositeMergeHandler.findForceMerges(1));
        assertTrue(exception.getMessage().contains("Test exception"));
    }

    public void testRegisterMerge() {
        Segment segment = mock(Segment.class);
        OneMerge oneMerge = new OneMerge(Arrays.asList(segment));

        CompositeEngine.ReleasableRef<CatalogSnapshot> mockRef = mock(CompositeEngine.ReleasableRef.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockRef.getRef()).thenReturn(mockSnapshot);
        when(mockSnapshot.getSegments()).thenReturn(Arrays.asList(segment));
        when(compositeEngine.acquireSnapshot()).thenReturn(mockRef);

        compositeMergeHandler.registerMerge(oneMerge);

        assertTrue(compositeMergeHandler.hasPendingMerges());
    }

    public void testOnMergeFinished() {
        Segment segment = mock(Segment.class);
        OneMerge oneMerge = new OneMerge(Arrays.asList(segment));

        CompositeEngine.ReleasableRef<CatalogSnapshot> mockRef = mock(CompositeEngine.ReleasableRef.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockRef.getRef()).thenReturn(mockSnapshot);
        when(mockSnapshot.getSegments()).thenReturn(Arrays.asList(segment));
        when(compositeEngine.acquireSnapshot()).thenReturn(mockRef);

        compositeMergeHandler.registerMerge(oneMerge);
        assertTrue(compositeMergeHandler.hasPendingMerges());

        compositeMergeHandler.onMergeFinished(oneMerge);
        assertFalse(compositeMergeHandler.hasPendingMerges());
    }

    public void testOnMergeFailure() {
        Segment segment = mock(Segment.class);
        OneMerge oneMerge = new OneMerge(Arrays.asList(segment));

        CompositeEngine.ReleasableRef<CatalogSnapshot> mockRef = mock(CompositeEngine.ReleasableRef.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockRef.getRef()).thenReturn(mockSnapshot);
        when(mockSnapshot.getSegments()).thenReturn(Arrays.asList(segment));
        when(compositeEngine.acquireSnapshot()).thenReturn(mockRef);

        compositeMergeHandler.registerMerge(oneMerge);
        assertTrue(compositeMergeHandler.hasPendingMerges());

        compositeMergeHandler.onMergeFailure(oneMerge);
        assertFalse(compositeMergeHandler.hasPendingMerges());
    }
}
