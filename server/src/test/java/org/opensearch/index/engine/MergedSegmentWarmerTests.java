/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.util.StringHelper;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.merge.MergedSegmentTransferTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MergedSegmentWarmerTests extends OpenSearchTestCase {

    IndexShard mockIndexShard;
    MergedSegmentWarmer mergedSegmentWarmer;
    @Mock
    SegmentCommitInfo segmentCommitInfo;
    @Mock
    MergedSegmentTransferTracker mergedSegmentTransferTracker;
    @Mock
    DiscoveryNodes mockDiscoveryNodes;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockIndexShard = mock(IndexShard.class);
        mergedSegmentTransferTracker = mock(MergedSegmentTransferTracker.class);

        ClusterState mockClusterState = mock(ClusterState.class);
        mockDiscoveryNodes = mock(DiscoveryNodes.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.nodes()).thenReturn(mockDiscoveryNodes);
        when(mockDiscoveryNodes.getMinNodeVersion()).thenReturn(Version.V_3_4_0);

        when(mockIndexShard.mergedSegmentTransferTracker()).thenReturn(mergedSegmentTransferTracker);
        when(mockIndexShard.shardId()).thenReturn(new ShardId(new Index("test-index", "_na_"), 0));
        mergedSegmentWarmer = new MergedSegmentWarmer(null, null, mockClusterService, mockIndexShard);
        segmentCommitInfo = new SegmentCommitInfo(segmentInfo(), 0, 0, 0, 0, 0, null);
    }

    public void testShouldWarm_warmerEnabled() throws IOException {
        RecoverySettings mockRecoverySettings = mock(RecoverySettings.class);
        when(mockRecoverySettings.isMergedSegmentReplicationWarmerEnabled()).thenReturn(true);
        when(mockRecoverySettings.getMergedSegmentWarmerMinSegmentSizeThreshold()).thenReturn(new ByteSizeValue(500, ByteSizeUnit.MB));
        when(mockIndexShard.getRecoverySettings()).thenReturn(mockRecoverySettings);
        when(segmentCommitInfo.info.dir.fileLength(any())).thenReturn(600 * 1_000_000L);

        assertTrue(
            "MergedSegmentWarmer#shouldWarm is expected to return true when merged segment warmer is enabled",
            mergedSegmentWarmer.shouldWarm(segmentCommitInfo)
        );
    }

    public void testShouldWarm_warmerDisabled() throws IOException {
        RecoverySettings mockRecoverySettings = mock(RecoverySettings.class);
        when(mockRecoverySettings.isMergedSegmentReplicationWarmerEnabled()).thenReturn(false);
        when(mockRecoverySettings.getMergedSegmentWarmerMinSegmentSizeThreshold()).thenReturn(new ByteSizeValue(500, ByteSizeUnit.MB));
        when(mockIndexShard.getRecoverySettings()).thenReturn(mockRecoverySettings);
        when(segmentCommitInfo.info.dir.fileLength(any())).thenReturn(600 * 1_000_000L);

        assertFalse(
            "MergedSegmentWarmer#shouldWarm is expected to return false when merged segment warmer is disabled",
            mergedSegmentWarmer.shouldWarm(segmentCommitInfo)
        );
    }

    public void testShouldWarm_segmentSizeLessThanThreshold() throws IOException {
        RecoverySettings mockRecoverySettings = mock(RecoverySettings.class);
        when(mockRecoverySettings.isMergedSegmentReplicationWarmerEnabled()).thenReturn(true);
        when(mockRecoverySettings.getMergedSegmentWarmerMinSegmentSizeThreshold()).thenReturn(new ByteSizeValue(500, ByteSizeUnit.MB));
        when(mockIndexShard.getRecoverySettings()).thenReturn(mockRecoverySettings);
        when(segmentCommitInfo.info.dir.fileLength(any())).thenReturn(150 * 1_000_000L);

        assertFalse(
            "MergedSegmentWarmer#shouldWarm is expected to return true when merged segment warmer is enabled",
            mergedSegmentWarmer.shouldWarm(segmentCommitInfo)
        );
    }

    public void testShouldWarm_segmentSizeGreaterThanThreshold() throws IOException {
        RecoverySettings mockRecoverySettings = mock(RecoverySettings.class);
        when(mockRecoverySettings.isMergedSegmentReplicationWarmerEnabled()).thenReturn(true);
        when(mockRecoverySettings.getMergedSegmentWarmerMinSegmentSizeThreshold()).thenReturn(new ByteSizeValue(500, ByteSizeUnit.MB));
        when(mockIndexShard.getRecoverySettings()).thenReturn(mockRecoverySettings);
        when(segmentCommitInfo.info.dir.fileLength(any())).thenReturn(150 * 1_000_000L);

        assertFalse(
            "MergedSegmentWarmer#shouldWarm is expected to return true when merged segment warmer is enabled",
            mergedSegmentWarmer.shouldWarm(segmentCommitInfo)
        );
    }

    public void testShouldWarm_minNodeVersionNotSatisfied() throws IOException {
        RecoverySettings mockRecoverySettings = mock(RecoverySettings.class);
        when(mockRecoverySettings.isMergedSegmentReplicationWarmerEnabled()).thenReturn(true);
        when(mockRecoverySettings.getMergedSegmentWarmerMinSegmentSizeThreshold()).thenReturn(new ByteSizeValue(500, ByteSizeUnit.MB));
        when(mockIndexShard.getRecoverySettings()).thenReturn(mockRecoverySettings);
        when(segmentCommitInfo.info.dir.fileLength(any())).thenReturn(600 * 1_000_000L);
        when(mockDiscoveryNodes.getMinNodeVersion()).thenReturn(Version.V_3_3_0);

        assertFalse(
            "MergedSegmentWarmer#shouldWarm is expected to return false when min node version requirements are not satisfied",
            mergedSegmentWarmer.shouldWarm(segmentCommitInfo)
        );
    }

    public void testShouldWarm_failure() throws IOException {
        MergedSegmentWarmer warmer = spy(new MergedSegmentWarmer(null, null, null, mockIndexShard));
        doThrow(new RuntimeException("test exception")).when(warmer).shouldWarm(any());
        doReturn(mock(SegmentCommitInfo.class)).when(warmer).segmentCommitInfo(any());
        warmer.warm(mock(LeafReader.class));
        verify(mergedSegmentTransferTracker, times(0)).incrementOngoingWarms();
        verify(mergedSegmentTransferTracker, times(0)).incrementTotalWarmInvocationsCount();
        verify(mergedSegmentTransferTracker, times(0)).addTotalWarmTimeMillis(ArgumentMatchers.anyLong());
        verify(mergedSegmentTransferTracker, times(0)).decrementOngoingWarms();
        verify(mergedSegmentTransferTracker, times(1)).incrementTotalWarmFailureCount();
    }

    private SegmentInfo segmentInfo() throws IOException {
        Directory dir = mock(MockDirectoryWrapper.class);
        Codec codec = Codec.getDefault();

        byte[] id = StringHelper.randomId();
        SegmentInfo info = new SegmentInfo(
            dir,
            Version.CURRENT.luceneVersion,
            Version.V_3_4_0.luceneVersion,
            "_123",
            1,
            true,
            false,
            codec,
            Collections.emptyMap(),
            id,
            Collections.emptyMap(),
            new Sort()
        );
        info.setFiles(List.of("_foo.si"));
        when(dir.fileLength(any())).thenReturn(100L);
        return info;
    }
}
