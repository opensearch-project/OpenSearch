/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.close;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.TimeUnit;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportCloseIndexActionTests extends OpenSearchTestCase {
    private static ThreadPool threadPool;
    private ClusterService clusterService;
    private final static String MIXED_MODE = "mixed";
    private final static String REMOTE_STORE_DIRECTION = "remote_store";
    private ClusterSettings clusterSettings;
    private final static String TEST_IND = "ind";

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(getTestClass().getName());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder()
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), MIXED_MODE)
            .put(MIGRATION_DIRECTION_SETTING.getKey(), REMOTE_STORE_DIRECTION)
            .build();
        ClusterSettings clusSet = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusSet);
        clusterSettings = clusterService.getClusterSettings();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    private TransportCloseIndexAction createAction() {
        return new TransportCloseIndexAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(MetadataIndexStateService.class),
            clusterSettings,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            new DestructiveOperations(Settings.EMPTY, clusterSettings)
        );
    }

    // Test if validateRemoteMigration throws illegal exception when compatibility mode is MIXED and migration Direction is REMOTE_STORE
    public void testRemoteValidation() {
        TransportCloseIndexAction action = createAction();

        Exception e = expectThrows(IllegalStateException.class, () -> action.doExecute(null, new CloseIndexRequest(TEST_IND), null));

        assertEquals("Cannot close index while remote migration is ongoing", e.getMessage());
    }
}
