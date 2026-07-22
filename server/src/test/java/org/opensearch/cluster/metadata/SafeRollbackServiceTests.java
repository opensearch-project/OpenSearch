/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContextAccess;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Collections.emptyMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SafeRollbackServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUpTest() throws Exception {
        threadPool = new TestThreadPool("SafeRollbackServiceTests");
        clusterService = createClusterService(threadPool);
    }

    @After
    public void tearDownTest() throws Exception {
        threadPool.shutdownNow();
        clusterService.close();
    }

    @SuppressWarnings("unchecked")
    public void testSafeRollbackAutoDisableWhenAllNodesUpgraded() throws Exception {
        Client mockClient = mock(Client.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        ClusterAdminClient mockClusterAdminClient = mock(ClusterAdminClient.class);
        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.cluster()).thenReturn(mockClusterAdminClient);

        final boolean[] updateCalled = new boolean[1];
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ClusterUpdateSettingsRequest request = (ClusterUpdateSettingsRequest) args[0];
            Boolean settingVal = request.persistentSettings().getAsBoolean(Metadata.SETTING_SAFE_ROLLBACK_ENABLED_SETTING.getKey(), null);
            assertThat(settingVal, equalTo(false));
            updateCalled[0] = true;
            ClusterUpdateSettingsResponse response = mock(ClusterUpdateSettingsResponse.class);
            try {
                java.lang.reflect.Field f = org.opensearch.action.support.clustermanager.AcknowledgedResponse.class.getDeclaredField(
                    "acknowledged"
                );
                f.setAccessible(true);
                f.set(response, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            ActionListener<ClusterUpdateSettingsResponse> listener = (ActionListener<ClusterUpdateSettingsResponse>) args[1];
            listener.onResponse(response);
            return null;
        }).when(mockClusterAdminClient).updateSettings(any(ClusterUpdateSettingsRequest.class), any(ActionListener.class));

        SafeRollbackService service = new SafeRollbackService(clusterService, mockClient, threadPool);

        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE)),
            Version.CURRENT
        );

        Metadata metadata = Metadata.builder()
            .persistentSettings(
                org.opensearch.common.settings.Settings.builder().put(Metadata.SETTING_SAFE_ROLLBACK_ENABLED_SETTING.getKey(), true).build()
            )
            .build();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().add(node1).localNodeId("node1").clusterManagerNodeId("node1").build())
            .metadata(metadata)
            .build();

        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            ThreadContextAccess.doPrivilegedVoid(threadContext::markAsSystemContext);
            service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        }

        assertTrue(updateCalled[0]);
    }

    public void testSafeRollbackDoesNotTriggerWhenOldNodesPresent() throws Exception {
        Client mockClient = mock(Client.class);
        SafeRollbackService service = new SafeRollbackService(clusterService, mockClient, threadPool);

        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE)),
            Version.CURRENT
        );
        Version olderVersion = Version.CURRENT.minimumIndexCompatibilityVersion(); // Previous version (e.g. 2.19.0)
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            "node2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            new HashSet<>(Collections.singleton(DiscoveryNodeRole.DATA_ROLE)),
            olderVersion
        );

        Metadata metadata = Metadata.builder()
            .persistentSettings(
                org.opensearch.common.settings.Settings.builder().put(Metadata.SETTING_SAFE_ROLLBACK_ENABLED_SETTING.getKey(), true).build()
            )
            .build();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).localNodeId("node1").clusterManagerNodeId("node1").build())
            .metadata(metadata)
            .build();

        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        // Should not fail and should not attempt update since node2 is older version
    }
}
