/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.identity.PluginSubject;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RestoreListenerPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RestoreListenerPluginIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(RecordingRestoreListenerPlugin.class);
        return plugins;
    }

    @Before
    public void resetRecordedCalls() {
        RecordingRestoreListenerPlugin.CALLS.clear();
    }

    public void testClusterManagerNotifiesPluginOnceAfterRestore() throws Exception {
        final String clusterManager = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createRepository("test-repo", "fs");
        createIndexWithRandomDocs("test-idx-1", randomIntBetween(1, 10));
        createIndexWithRandomDocs("test-idx-2", randomIntBetween(1, 10));
        createFullSnapshot("test-repo", "test-snap");
        assertAcked(client().admin().indices().prepareDelete("test-idx-1", "test-idx-2"));

        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertEquals(response.getRestoreInfo().totalShards(), response.getRestoreInfo().successfulShards());

        assertBusy(() -> assertThat(RecordingRestoreListenerPlugin.CALLS, hasSize(1)));
        final RecordingRestoreListenerPlugin.Call call = RecordingRestoreListenerPlugin.CALLS.get(0);
        assertEquals("only the elected cluster manager notifies plugins", clusterManager, call.nodeName());
        assertEquals("test-snap", call.context().snapshot().getSnapshotId().getName());
        assertFalse(call.context().partialFailure());
        assertTrue("core assigned the plugin a subject", call.hadSubject());
        assertFalse("the plugin does not run with the cluster manager's system context", call.systemContext());
        final Metadata metadata = clusterService().state().metadata();
        assertEquals(
            List.of(metadata.index("test-idx-1").getIndex(), metadata.index("test-idx-2").getIndex()),
            call.context().restoredIndices()
        );

        // Later cluster state changes do not report the same restore again.
        createIndex("unrelated-idx");
        ensureGreen("unrelated-idx");
        assertThat(RecordingRestoreListenerPlugin.CALLS, hasSize(1));
    }

    public void testRestoreThatDoesNotWaitForCompletionIsReportedUnderItsNewNames() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository("test-repo", "fs");
        createIndexWithRandomDocs("test-idx", randomIntBetween(1, 10));
        createFullSnapshot("test-repo", "test-snap");

        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("test-idx")
            .setRenameReplacement("restored-idx")
            .setWaitForCompletion(false)
            .get();
        assertEquals(RestStatus.ACCEPTED, response.status());

        assertBusy(() -> assertThat(RecordingRestoreListenerPlugin.CALLS, hasSize(1)));
        final List<Index> restored = RecordingRestoreListenerPlugin.CALLS.get(0).context().restoredIndices();
        assertEquals(List.of(clusterService().state().metadata().index("restored-idx").getIndex()), restored);
    }

    /**
     * Records every call it gets, with the name of the node it ran on and the identity it ran with.
     */
    public static class RecordingRestoreListenerPlugin extends Plugin implements RestoreListenerPlugin {

        record Call(String nodeName, RestoreContext context, boolean hadSubject, boolean systemContext) {
        }

        static final List<Call> CALLS = new CopyOnWriteArrayList<>();

        private final String nodeName;

        private volatile ThreadPool threadPool;

        private volatile PluginSubject subject;

        public RecordingRestoreListenerPlugin(Settings settings) {
            this.nodeName = settings.get("node.name");
        }

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            this.threadPool = threadPool;
            return Collections.emptyList();
        }

        @Override
        public void assignSubject(PluginSubject subject) {
            this.subject = subject;
        }

        @Override
        public void onRestore(RestoreContext context) {
            CALLS.add(new Call(nodeName, context, subject != null, threadPool.getThreadContext().isSystemContext()));
        }
    }
}
