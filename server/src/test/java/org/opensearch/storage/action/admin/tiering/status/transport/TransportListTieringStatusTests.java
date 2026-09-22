/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusRequest;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.storage.action.tiering.status.transport.TransportListTieringStatusAction;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportListTieringStatusTests extends OpenSearchTestCase {

    @Mock
    private TransportService transportService;
    @Mock
    private ClusterService clusterService;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private HotToWarmTieringService hotToWarmTieringService;
    @Mock
    private WarmToHotTieringService warmToHotTieringService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;
    @Mock
    private ClusterState clusterState;
    @Mock
    private ClusterBlocks clusterBlocks;
    @Mock
    private ActionListener<ListTieringStatusResponse> listener;

    private TransportListTieringStatusAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        action = new TransportListTieringStatusAction(
            transportService,
            clusterService,
            threadPool,
            hotToWarmTieringService,
            warmToHotTieringService,
            actionFilters,
            indexNameExpressionResolver
        );

        when(clusterState.blocks()).thenReturn(clusterBlocks);
    }

    public void testExecutor() {
        assertEquals(ThreadPool.Names.SAME, action.executor());
    }

    public void testClusterManagerOperation_WithTargetTierWarm() throws IOException {
        // Arrange
        ListTieringStatusRequest request = new ListTieringStatusRequest("WARM");

        List<TieringStatus> expectedStatus = Arrays.asList(createDummyTieringStatus("index1"), createDummyTieringStatus("index2"));

        when(hotToWarmTieringService.listTieringStatus()).thenReturn(expectedStatus);

        // Act
        action.clusterManagerOperation(request, clusterState, listener);

        // Assert
        verify(listener).onResponse(
            argThat(response -> response.getTieringStatusList().size() == 2 && response.getTieringStatusList().equals(expectedStatus))
        );
    }

    public void testClusterManagerOperation_WithTargetTierHot() throws IOException {
        // Arrange
        ListTieringStatusRequest request = new ListTieringStatusRequest("HOT");

        List<TieringStatus> expectedStatus = Arrays.asList(createDummyTieringStatus("index1"), createDummyTieringStatus("index2"));

        when(warmToHotTieringService.listTieringStatus()).thenReturn(expectedStatus);

        // Act
        action.clusterManagerOperation(request, clusterState, listener);

        // Assert
        verify(listener).onResponse(
            argThat(response -> response.getTieringStatusList().size() == 2 && response.getTieringStatusList().equals(expectedStatus))
        );
    }

    public void testClusterManagerOperation_WithoutTargetTier() throws IOException {
        // Arrange
        String targetTier = null;
        ListTieringStatusRequest request = new ListTieringStatusRequest(targetTier);

        List<TieringStatus> hotToWarmStatus = Arrays.asList(createDummyTieringStatus("index1"));
        List<TieringStatus> warmToHotStatus = Arrays.asList(createDummyTieringStatus("index2"));

        when(hotToWarmTieringService.listTieringStatus()).thenReturn(hotToWarmStatus);
        when(warmToHotTieringService.listTieringStatus()).thenReturn(warmToHotStatus);

        // Act
        action.clusterManagerOperation(request, clusterState, listener);

        // Assert
        verify(listener).onResponse(
            argThat(
                response -> response.getTieringStatusList().size() == 2
                    && response.getTieringStatusList().containsAll(hotToWarmStatus)
                    && response.getTieringStatusList().containsAll(warmToHotStatus)
            )
        );
    }

    public void testCheckBlock() {
        String targetTier = null;
        // Arrange
        ListTieringStatusRequest request = new ListTieringStatusRequest(targetTier);

        // Act
        ClusterBlockException result = action.checkBlock(request, clusterState);

        // Assert
        verify(clusterBlocks).globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    private TieringStatus createDummyTieringStatus(String indexName) throws IOException {
        TieringStatus status = new TieringStatus();
        status.setIndexName(indexName);
        status.setStatus("RUNNING");
        status.setStartTime(System.currentTimeMillis());
        status.setSource("hot");
        status.setTarget("warm");

        Map<String, Integer> shardStatusMap = new HashMap<>();
        shardStatusMap.put("COMPLETED", 0);
        shardStatusMap.put("IN_PROGRESS", 1);
        status.setShardLevelStatus(new TieringStatus.ShardLevelStatus(shardStatusMap, null));

        return status;
    }

    public void testReadResponse() throws Exception {
        ListTieringStatusResponse original = new ListTieringStatusResponse(Collections.emptyList());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();

        ListTieringStatusResponse deserialized = new ListTieringStatusResponse(in);
        assertTrue(deserialized.getTieringStatusList().isEmpty());
    }
}
