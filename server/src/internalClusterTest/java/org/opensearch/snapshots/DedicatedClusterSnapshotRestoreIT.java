/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Priority;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.index.seqno.RetentionLeaseActions;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.admin.cluster.RestClusterStateAction;
import org.opensearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.TestCustomMetadata;
import org.opensearch.test.disruption.BusyClusterManagerServiceDisruption;
import org.opensearch.test.disruption.ServiceDisruptionScheme;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.opensearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.opensearch.test.NodeRoles.nonClusterManagerNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFutureThrows;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DedicatedClusterSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    public static class TestCustomMetadataPlugin extends Plugin {

        private final List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>();
        private final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();

        public TestCustomMetadataPlugin() {
            registerBuiltinWritables();
        }

        private <T extends Metadata.Custom> void registerMetadataCustom(
            String name,
            Writeable.Reader<T> reader,
            Writeable.Reader<NamedDiff> diffReader,
            CheckedFunction<XContentParser, T, IOException> parser
        ) {
            namedWritables.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, name, reader));
            namedWritables.add(new NamedWriteableRegistry.Entry(NamedDiff.class, name, diffReader));
            namedXContents.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(name), parser));
        }

        private void registerBuiltinWritables() {
            registerMetadataCustom(
                SnapshottableMetadata.TYPE,
                SnapshottableMetadata::readFrom,
                SnapshottableMetadata::readDiffFrom,
                SnapshottableMetadata::fromXContent
            );
            registerMetadataCustom(
                NonSnapshottableMetadata.TYPE,
                NonSnapshottableMetadata::readFrom,
                NonSnapshottableMetadata::readDiffFrom,
                NonSnapshottableMetadata::fromXContent
            );
            registerMetadataCustom(
                SnapshottableGatewayMetadata.TYPE,
                SnapshottableGatewayMetadata::readFrom,
                SnapshottableGatewayMetadata::readDiffFrom,
                SnapshottableGatewayMetadata::fromXContent
            );
            registerMetadataCustom(
                NonSnapshottableGatewayMetadata.TYPE,
                NonSnapshottableGatewayMetadata::readFrom,
                NonSnapshottableGatewayMetadata::readDiffFrom,
                NonSnapshottableGatewayMetadata::fromXContent
            );
            registerMetadataCustom(
                SnapshotableGatewayNoApiMetadata.TYPE,
                SnapshotableGatewayNoApiMetadata::readFrom,
                NonSnapshottableGatewayMetadata::readDiffFrom,
                SnapshotableGatewayNoApiMetadata::fromXContent
            );
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return namedWritables;
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return namedXContents;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockRepository.Plugin.class,
            TestCustomMetadataPlugin.class,
            BrokenSettingPlugin.class,
            MockTransportService.TestPlugin.class
        );
    }

    public static class BrokenSettingPlugin extends Plugin {
        private static boolean breakSetting = false;
        private static final IllegalArgumentException EXCEPTION = new IllegalArgumentException("this setting goes boom");

        static void breakSetting(boolean breakSetting) {
            BrokenSettingPlugin.breakSetting = breakSetting;
        }

        static final Setting<String> BROKEN_SETTING = new Setting<>("setting.broken", "default", s -> s, s -> {
            if ((s.equals("default") == false && breakSetting)) {
                throw EXCEPTION;
            }
        }, Setting.Property.NodeScope, Setting.Property.Dynamic);

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(BROKEN_SETTING);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37485")
    public void testExceptionWhenRestoringPersistentSettings() {
        logger.info("--> start 2 nodes");
        internalCluster().startNodes(2);

        Client client = client();
        Consumer<String> setSettingValue = value -> {
            client.admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(BrokenSettingPlugin.BROKEN_SETTING.getKey(), value))
                .execute()
                .actionGet();
        };

        Consumer<String> assertSettingValue = value -> {
            assertThat(
                client.admin()
                    .cluster()
                    .prepareState()
                    .setRoutingTable(false)
                    .setNodes(false)
                    .execute()
                    .actionGet()
                    .getState()
                    .getMetadata()
                    .persistentSettings()
                    .get(BrokenSettingPlugin.BROKEN_SETTING.getKey()),
                equalTo(value)
            );
        };

        logger.info("--> set test persistent setting");
        setSettingValue.accept("new value");
        assertSettingValue.accept("new value");

        createRepository("test-repo", "fs");
        createFullSnapshot("test-repo", "test-snap");
        assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> change the test persistent setting and break it");
        setSettingValue.accept("new value 2");
        assertSettingValue.accept("new value 2");
        BrokenSettingPlugin.breakSetting(true);

        logger.info("--> restore snapshot");
        try {
            client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setRestoreGlobalState(true)
                .setWaitForCompletion(true)
                .execute()
                .actionGet();

        } catch (IllegalArgumentException ex) {
            assertEquals(BrokenSettingPlugin.EXCEPTION.getMessage(), ex.getMessage());
        }

        assertSettingValue.accept("new value 2");
    }

    public void testRestoreCustomMetadata() throws Exception {
        Path tempDir = randomRepoPath();

        logger.info("--> start node");
        internalCluster().startNode();
        createIndex("test-idx");
        logger.info("--> add custom persistent metadata");
        updateClusterState(currentState -> {
            ClusterState.Builder builder = ClusterState.builder(currentState);
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("before_snapshot_s"));
            metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("before_snapshot_ns"));
            metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("before_snapshot_s_gw"));
            metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("before_snapshot_ns_gw"));
            metadataBuilder.putCustom(
                SnapshotableGatewayNoApiMetadata.TYPE,
                new SnapshotableGatewayNoApiMetadata("before_snapshot_s_gw_noapi")
            );
            builder.metadata(metadataBuilder);
            return builder.build();
        });

        createRepository("test-repo", "fs", tempDir);
        createFullSnapshot("test-repo", "test-snap");
        assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> change custom persistent metadata");
        updateClusterState(currentState -> {
            ClusterState.Builder builder = ClusterState.builder(currentState);
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            if (randomBoolean()) {
                metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("after_snapshot_s"));
            } else {
                metadataBuilder.removeCustom(SnapshottableMetadata.TYPE);
            }
            metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("after_snapshot_ns"));
            if (randomBoolean()) {
                metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("after_snapshot_s_gw"));
            } else {
                metadataBuilder.removeCustom(SnapshottableGatewayMetadata.TYPE);
            }
            metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("after_snapshot_ns_gw"));
            metadataBuilder.removeCustom(SnapshotableGatewayNoApiMetadata.TYPE);
            builder.metadata(metadataBuilder);
            return builder.build();
        });

        logger.info("--> delete repository");
        assertAcked(clusterAdmin().prepareDeleteRepository("test-repo"));

        createRepository("test-repo-2", "fs", tempDir);

        logger.info("--> restore snapshot");
        clusterAdmin().prepareRestoreSnapshot("test-repo-2", "test-snap")
            .setRestoreGlobalState(true)
            .setIndices("-*")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();

        logger.info("--> make sure old repository wasn't restored");
        assertRequestBuilderThrows(clusterAdmin().prepareGetRepositories("test-repo"), RepositoryMissingException.class);
        assertThat(clusterAdmin().prepareGetRepositories("test-repo-2").get().repositories().size(), equalTo(1));

        logger.info("--> check that custom persistent metadata was restored");
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        Metadata metadata = clusterState.getMetadata();
        assertThat(((SnapshottableMetadata) metadata.custom(SnapshottableMetadata.TYPE)).getData(), equalTo("before_snapshot_s"));
        assertThat(((NonSnapshottableMetadata) metadata.custom(NonSnapshottableMetadata.TYPE)).getData(), equalTo("after_snapshot_ns"));
        assertThat(
            ((SnapshottableGatewayMetadata) metadata.custom(SnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw")
        );
        assertThat(
            ((NonSnapshottableGatewayMetadata) metadata.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("after_snapshot_ns_gw")
        );

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> check that gateway-persistent custom metadata survived full cluster restart");
        clusterState = clusterAdmin().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        metadata = clusterState.getMetadata();
        assertThat(metadata.custom(SnapshottableMetadata.TYPE), nullValue());
        assertThat(metadata.custom(NonSnapshottableMetadata.TYPE), nullValue());
        assertThat(
            ((SnapshottableGatewayMetadata) metadata.custom(SnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw")
        );
        assertThat(
            ((NonSnapshottableGatewayMetadata) metadata.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("after_snapshot_ns_gw")
        );
        // Shouldn't be returned as part of API response
        assertThat(metadata.custom(SnapshotableGatewayNoApiMetadata.TYPE), nullValue());
        // But should still be in state
        metadata = internalCluster().getInstance(ClusterService.class).state().metadata();
        assertThat(
            ((SnapshotableGatewayNoApiMetadata) metadata.custom(SnapshotableGatewayNoApiMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw_noapi")
        );
    }

    public void testSnapshotDuringNodeShutdown() throws Exception {
        assertAcked(prepareCreate("test-idx", 2, indexSettingsNoReplicas(2)));
        ensureGreen();

        indexRandomDocs("test-idx", 100);
        final Path repoPath = randomRepoPath();
        createRepository(
            "test-repo",
            "mock",
            Settings.builder().put("location", repoPath).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], shutting it down", blockedNode);
        unblockNode("test-repo", blockedNode);

        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        logger.info("--> waiting for completion");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(60));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");
    }

    public void testSnapshotWithStuckNode() throws Exception {
        logger.info("--> start 2 nodes");
        List<String> nodes = internalCluster().startNodes(2);

        assertAcked(prepareCreate("test-idx", 2, indexSettingsNoReplicas(2)));
        ensureGreen();

        indexRandomDocs("test-idx", 100);

        Path repo = randomRepoPath();
        createRepository(
            "test-repo",
            "mock",
            Settings.builder().put("location", repo).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");
        // Remove it from the list of available nodes
        nodes.remove(blockedNode);

        assertFileCount(repo, 0);
        logger.info("--> snapshot");
        clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], aborting snapshot", blockedNode);

        ActionFuture<AcknowledgedResponse> deleteSnapshotResponseFuture = internalCluster().client(nodes.get(0))
            .admin()
            .cluster()
            .prepareDeleteSnapshot("test-repo", "test-snap")
            .execute();
        // Make sure that abort makes some progress
        Thread.sleep(100);
        unblockNode("test-repo", blockedNode);
        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        try {
            assertAcked(deleteSnapshotResponseFuture.actionGet());
        } catch (SnapshotMissingException ex) {
            // When cluster-manager node is closed during this test, it sometime manages to delete the snapshot files before
            // completely stopping. In this case the retried delete snapshot operation on the new cluster-manager can fail
            // with SnapshotMissingException
        }

        logger.info("--> making sure that snapshot no longer exists");
        expectThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet()
        );

        logger.info("--> Go through a loop of creating and deleting a snapshot to trigger repository cleanup");
        clusterAdmin().prepareCleanupRepository("test-repo").get();

        // Expect two files to remain in the repository:
        // (1) index-(N+1)
        // (2) index-latest
        assertFileCount(repo, 2);
        logger.info("--> done");
    }

    public void testRestoreIndexWithMissingShards() throws Exception {
        disableRepoConsistencyCheck("This test leaves behind a purposely broken repository");
        logger.info("--> start 2 nodes");
        internalCluster().startNode();
        internalCluster().startNode();
        cluster().wipeIndices("_all");

        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx-some", 2, indexSettingsNoReplicas(6)));
        ensureGreen();

        indexRandomDocs("test-idx-some", 100);

        logger.info("--> shutdown one of the nodes");
        internalCluster().stopRandomDataNode();
        assertThat(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout("1m")
                .setWaitForNodes("<2")
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        logger.info("--> create an index that will have all allocated shards");
        assertAcked(prepareCreate("test-idx-all", 1, indexSettingsNoReplicas(6)));
        ensureGreen("test-idx-all");

        logger.info("--> create an index that will be closed");
        assertAcked(prepareCreate("test-idx-closed", 1, indexSettingsNoReplicas(4)));
        indexRandomDocs("test-idx-all", 100);
        indexRandomDocs("test-idx-closed", 100);
        assertAcked(client().admin().indices().prepareClose("test-idx-closed"));

        logger.info("--> create an index that will have no allocated shards");
        assertAcked(
            prepareCreate("test-idx-none", 1, indexSettingsNoReplicas(6).put("index.routing.allocation.include.tag", "nowhere"))
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .get()
        );
        assertTrue(indexExists("test-idx-none"));

        createRepository("test-repo", "fs");

        logger.info("--> start snapshot with default settings without a closed index - should fail");
        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            () -> clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(true)
                .execute()
                .actionGet()
        );
        assertThat(sne.getMessage(), containsString("Indices don't have primary shards"));

        if (randomBoolean()) {
            logger.info("checking snapshot completion using status");
            clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-2")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(false)
                .setPartial(true)
                .execute()
                .actionGet();
            assertBusy(() -> {
                SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo")
                    .setSnapshots("test-snap-2")
                    .get();
                List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
                assertEquals(snapshotStatuses.size(), 1);
                logger.trace("current snapshot status [{}]", snapshotStatuses.get(0));
                assertThat(getSnapshot("test-repo", "test-snap-2").state(), equalTo(SnapshotState.PARTIAL));
            }, 1, TimeUnit.MINUTES);
            SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo")
                .setSnapshots("test-snap-2")
                .get();
            List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
            assertThat(snapshotStatuses.size(), equalTo(1));
            SnapshotStatus snapshotStatus = snapshotStatuses.get(0);

            assertThat(snapshotStatus.getShardsStats().getTotalShards(), equalTo(22));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), lessThan(16));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), greaterThan(10));

            // There is slight delay between snapshot being marked as completed in the cluster state and on the file system
            // After it was marked as completed in the cluster state - we need to check if it's completed on the file system as well
            assertBusy(() -> {
                SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap-2");
                assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
            }, 1, TimeUnit.MINUTES);
        } else {
            logger.info("checking snapshot completion using wait_for_completion flag");
            final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-2")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(true)
                .setPartial(true)
                .execute()
                .actionGet();
            logger.info(
                "State: [{}], Reason: [{}]",
                createSnapshotResponse.getSnapshotInfo().state(),
                createSnapshotResponse.getSnapshotInfo().reason()
            );
            assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(22));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), lessThan(16));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(10));
            assertThat(getSnapshot("test-repo", "test-snap-2").state(), equalTo(SnapshotState.PARTIAL));
        }

        assertAcked(client().admin().indices().prepareClose("test-idx-all"));

        logger.info("--> restore incomplete snapshot - should fail");
        assertFutureThrows(
            clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false)
                .setWaitForCompletion(true)
                .execute(),
            SnapshotRestoreException.class
        );

        logger.info("--> restore snapshot for the index that was snapshotted completely");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-all")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertDocCount("test-idx-all", 100L);

        logger.info("--> restore snapshot for the partial index");
        cluster().wipeIndices("test-idx-some");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-some")
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), allOf(greaterThan(0), lessThan(6)));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), greaterThan(0));
        assertThat(getCountForIndex("test-idx-some"), allOf(greaterThan(0L), lessThan(100L)));

        logger.info("--> restore snapshot for the index that didn't have any shards snapshotted successfully");
        cluster().wipeIndices("test-idx-none");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-none")
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(6));
        assertThat(getCountForIndex("test-idx-some"), allOf(greaterThan(0L), lessThan(100L)));

        logger.info("--> restore snapshot for the closed index that was snapshotted completely");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-closed")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(4));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(4));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertDocCount("test-idx-closed", 100L);
    }

    public void testRestoreIndexWithShardsMissingInLocalGateway() throws Exception {
        logger.info("--> start 2 nodes");

        internalCluster().startNodes(2);
        cluster().wipeIndices("_all");

        createRepository("test-repo", "fs");

        int numberOfShards = 6;
        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx", 2, indexSettingsNoReplicas(numberOfShards)));
        ensureGreen();

        indexRandomDocs("test-idx", 100);

        logger.info("--> force merging down to a single segment to get a deterministic set of files");
        assertEquals(
            client().admin().indices().prepareForceMerge("test-idx").setMaxNumSegments(1).setFlush(true).get().getFailedShards(),
            0
        );

        createSnapshot("test-repo", "test-snap-1", Collections.singletonList("test-idx"));

        logger.info("--> close the index");
        assertAcked(client().admin().indices().prepareClose("test-idx"));

        logger.info("--> shutdown one of the nodes that should make half of the shards unavailable");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        assertThat(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout("1m")
                .setWaitForNodes("2")
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        logger.info("--> restore index snapshot");
        assertThat(
            clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-1")
                .setRestoreGlobalState(false)
                .setWaitForCompletion(true)
                .get()
                .getRestoreInfo()
                .successfulShards(),
            equalTo(6)
        );

        ensureGreen("test-idx");

        final Set<Integer> reusedShards = new HashSet<>();
        List<RecoveryState> recoveryStates = client().admin()
            .indices()
            .prepareRecoveries("test-idx")
            .get()
            .shardRecoveryStates()
            .get("test-idx");
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getIndex().reusedBytes() > 0) {
                reusedShards.add(recoveryState.getShardId().getId());
            }
        }
        logger.info("--> check that at least half of the shards had some reuse: [{}]", reusedShards);
        assertThat(reusedShards.size(), greaterThanOrEqualTo(numberOfShards / 2));
    }

    public void testRegistrationFailure() {
        disableRepoConsistencyCheck("This test does not create any data in the repository");
        logger.info("--> start first node");
        internalCluster().startNode();
        logger.info("--> start second node");
        // Make sure the first node is elected as cluster-manager
        internalCluster().startNode(nonClusterManagerNode());
        // Register mock repositories
        for (int i = 0; i < 5; i++) {
            OpenSearchIntegTestCase.putRepositoryRequestBuilder(
                clusterAdmin(),
                "test-repo" + i,
                "mock",
                false,
                Settings.builder().put("location", randomRepoPath()),
                null,
                false
            ).get();
        }
        logger.info("--> make sure that properly setup repository can be registered on all nodes");
        OpenSearchIntegTestCase.putRepositoryRequestBuilder(
            clusterAdmin(),
            "test-repo-0",
            "fs",
            true,
            Settings.builder().put("location", randomRepoPath()),
            null,
            false
        ).get();
    }

    public void testThatSensitiveRepositorySettingsAreNotExposed() throws Exception {
        disableRepoConsistencyCheck("This test does not create any data in the repository");
        logger.info("--> start two nodes");
        internalCluster().startNodes(2);
        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put(MockRepository.Plugin.USERNAME_SETTING.getKey(), "notsecretusername")
                .put(MockRepository.Plugin.PASSWORD_SETTING.getKey(), "verysecretpassword")
        );

        NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);
        RestGetRepositoriesAction getRepoAction = new RestGetRepositoriesAction(internalCluster().getInstance(SettingsFilter.class));
        RestRequest getRepoRequest = new FakeRestRequest();
        getRepoRequest.params().put("repository", "test-repo");
        final CountDownLatch getRepoLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> getRepoError = new AtomicReference<>();
        getRepoAction.handleRequest(getRepoRequest, new AbstractRestChannel(getRepoRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().utf8ToString(), containsString("notsecretusername"));
                    assertThat(response.content().utf8ToString(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    getRepoError.set(ex);
                }
                getRepoLatch.countDown();
            }
        }, nodeClient);
        assertTrue(getRepoLatch.await(1, TimeUnit.SECONDS));
        if (getRepoError.get() != null) {
            throw getRepoError.get();
        }

        RestClusterStateAction clusterStateAction = new RestClusterStateAction(internalCluster().getInstance(SettingsFilter.class));
        RestRequest clusterStateRequest = new FakeRestRequest();
        final CountDownLatch clusterStateLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> clusterStateError = new AtomicReference<>();
        clusterStateAction.handleRequest(clusterStateRequest, new AbstractRestChannel(clusterStateRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().utf8ToString(), containsString("notsecretusername"));
                    assertThat(response.content().utf8ToString(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    clusterStateError.set(ex);
                }
                clusterStateLatch.countDown();
            }
        }, nodeClient);
        assertTrue(clusterStateLatch.await(1, TimeUnit.SECONDS));
        if (clusterStateError.get() != null) {
            throw clusterStateError.get();
        }
    }

    public void testClusterManagerShutdownDuringSnapshot() throws Exception {
        logger.info("-->  starting two cluster-manager nodes and two data nodes");
        internalCluster().startClusterManagerOnlyNodes(2);
        internalCluster().startDataOnlyNodes(2);

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(between(1, 20))));
        ensureGreen();

        indexRandomDocs("test-idx", randomIntBetween(10, 100));

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        dataNodeClient().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();

        logger.info("--> stopping cluster-manager node");
        internalCluster().stopCurrentClusterManagerNode();

        logger.info("--> wait until the snapshot is done");

        assertBusy(() -> assertTrue(getSnapshot("test-repo", "test-snap").state().completed()), 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was successful");
        SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testClusterManagerAndDataShutdownDuringSnapshot() throws Exception {
        logger.info("-->  starting three cluster-manager nodes and two data nodes");
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startDataOnlyNodes(2);

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "mock", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(between(1, 20))));
        ensureGreen();

        indexRandomDocs("test-idx", randomIntBetween(10, 100));

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        final String clusterManagerNode = blockClusterManagerFromFinalizingSnapshotOnSnapFile("test-repo");
        final String dataNode = blockNodeWithIndex("test-repo", "test-idx");

        dataNodeClient().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();

        logger.info("--> stopping data node {}", dataNode);
        stopNode(dataNode);
        logger.info("--> stopping cluster-manager node {} ", clusterManagerNode);
        internalCluster().stopCurrentClusterManagerNode();

        logger.info("--> wait until the snapshot is done");

        assertBusy(() -> assertTrue(getSnapshot("test-repo", "test-snap").state().completed()), 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was partial");
        SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
        assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
        assertNotEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertThat(snapshotInfo.failedShards(), greaterThan(0));
        for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
            assertNotNull(failure.reason());
        }
    }

    /**
     * Tests that a shrunken index (created via the shrink APIs) and subsequently snapshotted
     * can be restored when the node the shrunken index was created on is no longer part of
     * the cluster.
     */
    public void testRestoreShrinkIndex() throws Exception {
        logger.info("-->  starting a cluster-manager node and a data node");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        final String repo = "test-repo";
        final String snapshot = "test-snap";
        final String sourceIdx = "test-idx";
        final String shrunkIdx = "test-idx-shrunk";

        createRepository(repo, "fs");

        assertAcked(prepareCreate(sourceIdx, 0, indexSettingsNoReplicas(between(2, 10))));
        ensureGreen();
        indexRandomDocs(sourceIdx, randomIntBetween(10, 100));

        logger.info("--> shrink the index");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(sourceIdx)
                .setSettings(Settings.builder().put("index.blocks.write", true))
                .get()
        );
        assertAcked(client().admin().indices().prepareResizeIndex(sourceIdx, shrunkIdx).get());

        logger.info("--> snapshot the shrunk index");
        createSnapshot(repo, snapshot, Collections.singletonList(shrunkIdx));

        logger.info("--> delete index and stop the data node");
        assertAcked(client().admin().indices().prepareDelete(sourceIdx).get());
        assertAcked(client().admin().indices().prepareDelete(shrunkIdx).get());
        internalCluster().stopRandomDataNode();
        clusterAdmin().prepareHealth().setTimeout("30s").setWaitForNodes("1");

        logger.info("--> start a new data node");
        final Settings dataSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLength(5))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()) // to get a new node id
            .build();
        internalCluster().startDataOnlyNode(dataSettings);
        clusterAdmin().prepareHealth().setTimeout("30s").setWaitForNodes("2");

        logger.info("--> restore the shrunk index and ensure all shards are allocated");
        RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(repo, snapshot)
            .setWaitForCompletion(true)
            .setIndices(shrunkIdx)
            .get();
        assertEquals(restoreResponse.getRestoreInfo().totalShards(), restoreResponse.getRestoreInfo().successfulShards());
        ensureYellow();
    }

    public void testSnapshotWithDateMath() {
        final String repo = "repo";

        final IndexNameExpressionResolver nameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        final String snapshotName = "<snapshot-{now/d}>";

        logger.info("-->  creating repository");
        createRepository(repo, "fs", Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()));

        final String expression1 = nameExpressionResolver.resolveDateMathExpression(snapshotName);
        logger.info("-->  creating date math snapshot");
        createFullSnapshot(repo, snapshotName);
        // snapshot could be taken before or after a day rollover
        final String expression2 = nameExpressionResolver.resolveDateMathExpression(snapshotName);

        SnapshotsStatusResponse response = clusterAdmin().prepareSnapshotStatus(repo)
            .setSnapshots(Sets.newHashSet(expression1, expression2).toArray(Strings.EMPTY_ARRAY))
            .setIgnoreUnavailable(true)
            .get();
        List<SnapshotStatus> snapshots = response.getSnapshots();
        assertThat(snapshots, hasSize(1));
        assertThat(snapshots.get(0).getState().completed(), equalTo(true));
    }

    public void testSnapshotTotalAndIncrementalSizes() throws Exception {
        final String indexName = "test-blocks-1";
        final String repositoryName = "repo-" + indexName;
        final String snapshot0 = "snapshot-0";
        final String snapshot1 = "snapshot-1";

        createIndex(indexName);

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "init").execute().actionGet();
        }

        final Path repoPath = randomRepoPath();
        createRepository(repositoryName, "fs", repoPath);
        createFullSnapshot(repositoryName, snapshot0);

        SnapshotsStatusResponse response = clusterAdmin().prepareSnapshotStatus(repositoryName).setSnapshots(snapshot0).get();

        List<SnapshotStatus> snapshots = response.getSnapshots();

        List<Path> snapshot0Files = scanSnapshotFolder(repoPath);
        assertThat(snapshots, hasSize(1));

        final int snapshot0FileCount = snapshot0Files.size();
        final long snapshot0FileSize = calculateTotalFilesSize(snapshot0Files);

        SnapshotStats stats = snapshots.get(0).getStats();

        final List<Path> snapshot0IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot0IndexMetaFiles, hasSize(1)); // snapshotting a single index
        assertThat(stats.getTotalFileCount(), greaterThanOrEqualTo(snapshot0FileCount));
        assertThat(stats.getTotalSize(), greaterThanOrEqualTo(snapshot0FileSize));

        assertThat(stats.getIncrementalFileCount(), equalTo(stats.getTotalFileCount()));
        assertThat(stats.getIncrementalSize(), equalTo(stats.getTotalSize()));

        assertThat(stats.getIncrementalFileCount(), equalTo(stats.getProcessedFileCount()));
        assertThat(stats.getIncrementalSize(), equalTo(stats.getProcessedSize()));

        // add few docs - less than initially
        docs = between(1, 5);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "test" + i).execute().actionGet();
        }

        // create another snapshot
        // total size has to grow and has to be equal to files on fs
        createFullSnapshot(repositoryName, snapshot1);

        // drop 1st one to avoid miscalculation as snapshot reuses some files of prev snapshot
        assertAcked(startDeleteSnapshot(repositoryName, snapshot0).get());

        response = clusterAdmin().prepareSnapshotStatus(repositoryName).setSnapshots(snapshot1).get();

        final List<Path> snapshot1Files = scanSnapshotFolder(repoPath);
        final List<Path> snapshot1IndexMetaFiles = findRepoMetaBlobs(repoPath);

        // The IndexMetadata did not change between snapshots, verify that no new redundant IndexMetaData was written to the repository
        assertThat(snapshot1IndexMetaFiles, is(snapshot0IndexMetaFiles));

        final int snapshot1FileCount = snapshot1Files.size();
        final long snapshot1FileSize = calculateTotalFilesSize(snapshot1Files);

        snapshots = response.getSnapshots();

        SnapshotStats anotherStats = snapshots.get(0).getStats();

        ArrayList<Path> snapshotFilesDiff = new ArrayList<>(snapshot1Files);
        snapshotFilesDiff.removeAll(snapshot0Files);

        assertThat(anotherStats.getIncrementalFileCount(), greaterThanOrEqualTo(snapshotFilesDiff.size()));
        assertThat(anotherStats.getIncrementalSize(), greaterThanOrEqualTo(calculateTotalFilesSize(snapshotFilesDiff)));

        assertThat(anotherStats.getIncrementalFileCount(), equalTo(anotherStats.getProcessedFileCount()));
        assertThat(anotherStats.getIncrementalSize(), equalTo(anotherStats.getProcessedSize()));

        assertThat(stats.getTotalSize(), lessThan(anotherStats.getTotalSize()));
        assertThat(stats.getTotalFileCount(), lessThan(anotherStats.getTotalFileCount()));

        assertThat(anotherStats.getTotalFileCount(), greaterThanOrEqualTo(snapshot1FileCount));
        assertThat(anotherStats.getTotalSize(), greaterThanOrEqualTo(snapshot1FileSize));
    }

    public void testDeduplicateIndexMetadata() throws Exception {
        final String indexName = "test-blocks-1";
        final String repositoryName = "repo-" + indexName;
        final String snapshot0 = "snapshot-0";
        final String snapshot1 = "snapshot-1";
        final String snapshot2 = "snapshot-2";

        createIndex(indexName);

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "init").execute().actionGet();
        }

        final Path repoPath = randomRepoPath();
        createRepository(repositoryName, "fs", repoPath);
        createFullSnapshot(repositoryName, snapshot0);

        final List<Path> snapshot0IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot0IndexMetaFiles, hasSize(1)); // snapshotting a single index

        docs = between(1, 5);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "test" + i).execute().actionGet();
        }

        logger.info("--> restart random data node and add new data node to change index allocation");
        internalCluster().restartRandomDataNode();
        internalCluster().startDataOnlyNode();
        ensureGreen(indexName);

        assertThat(
            client().admin().cluster().prepareCreateSnapshot(repositoryName, snapshot1).setWaitForCompletion(true).get().status(),
            equalTo(RestStatus.OK)
        );

        final List<Path> snapshot1IndexMetaFiles = findRepoMetaBlobs(repoPath);

        // The IndexMetadata did not change between snapshots, verify that no new redundant IndexMetaData was written to the repository
        assertThat(snapshot1IndexMetaFiles, is(snapshot0IndexMetaFiles));

        // index to some other field to trigger a change in index metadata
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("new_field", "test" + i).execute().actionGet();
        }
        createFullSnapshot(repositoryName, snapshot2);

        final List<Path> snapshot2IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot2IndexMetaFiles, hasSize(2)); // should have created one new metadata blob

        assertAcked(clusterAdmin().prepareDeleteSnapshot(repositoryName, snapshot0, snapshot1).get());
        final List<Path> snapshot3IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot3IndexMetaFiles, hasSize(1)); // should have deleted the metadata blob referenced by the first two snapshots
    }

    public void testDataNodeRestartWithBusyClusterManagerDuringSnapshot() throws Exception {
        logger.info("-->  starting a cluster-manager node and two data nodes");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "mock", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(5)));
        ensureGreen();
        indexRandomDocs("test-idx", randomIntBetween(50, 100));

        final String dataNode = blockNodeWithIndex("test-repo", "test-idx");
        logger.info("-->  snapshot");
        ServiceDisruptionScheme disruption = new BusyClusterManagerServiceDisruption(random(), Priority.HIGH);
        setDisruptionScheme(disruption);
        client(internalCluster().getClusterManagerName()).admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();
        disruption.startDisrupting();
        logger.info("-->  restarting data node, which should cause primary shards to be failed");
        internalCluster().restartNode(dataNode, InternalTestCluster.EMPTY_CALLBACK);

        logger.info("-->  wait for shard snapshots to show as failed");
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareSnapshotStatus("test-repo")
                    .setSnapshots("test-snap")
                    .get()
                    .getSnapshots()
                    .get(0)
                    .getShardsStats()
                    .getFailedShards(),
                greaterThanOrEqualTo(1)
            ),
            60L,
            TimeUnit.SECONDS
        );

        unblockNode("test-repo", dataNode);
        disruption.stopDisrupting();
        // check that snapshot completes
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap")
                .setIgnoreUnavailable(true)
                .get();
            assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
            assertTrue(snapshotInfo.state().toString(), snapshotInfo.state().completed());
        }, 60L, TimeUnit.SECONDS);
    }

    public void testDataNodeRestartAfterShardSnapshotFailure() throws Exception {
        logger.info("-->  starting a cluster-manager node and two data nodes");
        internalCluster().startClusterManagerOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "mock", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(2)));
        ensureGreen();
        indexRandomDocs("test-idx", randomIntBetween(50, 100));

        blockAllDataNodes("test-repo");
        logger.info("-->  snapshot");
        client(internalCluster().getClusterManagerName()).admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();
        logger.info("-->  restarting first data node, which should cause the primary shard on it to be failed");
        internalCluster().restartNode(dataNodes.get(0), InternalTestCluster.EMPTY_CALLBACK);

        logger.info("-->  wait for shard snapshot of first primary to show as failed");
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareSnapshotStatus("test-repo")
                    .setSnapshots("test-snap")
                    .get()
                    .getSnapshots()
                    .get(0)
                    .getShardsStats()
                    .getFailedShards(),
                is(1)
            ),
            60L,
            TimeUnit.SECONDS
        );

        logger.info("-->  restarting second data node, which should cause the primary shard on it to be failed");
        internalCluster().restartNode(dataNodes.get(1), InternalTestCluster.EMPTY_CALLBACK);

        // check that snapshot completes with both failed shards being accounted for in the snapshot result
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap")
                .setIgnoreUnavailable(true)
                .get();
            assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
            assertTrue(snapshotInfo.state().toString(), snapshotInfo.state().completed());
            assertThat(snapshotInfo.totalShards(), is(2));
            assertThat(snapshotInfo.shardFailures(), hasSize(2));
        }, 60L, TimeUnit.SECONDS);
    }

    public void testRetentionLeasesClearedOnRestore() throws Exception {
        final String repoName = "test-repo-retention-leases";
        createRepository(repoName, "fs");

        final String indexName = "index-retention-leases";
        final int shardCount = randomIntBetween(1, 5);
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(indexSettingsNoReplicas(shardCount)));
        final ShardId shardId = new ShardId(resolveIndex(indexName), randomIntBetween(0, shardCount - 1));

        final int snapshotDocCount = iterations(10, 1000);
        logger.debug("--> indexing {} docs into {}", snapshotDocCount, indexName);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[snapshotDocCount];
        for (int i = 0; i < snapshotDocCount; i++) {
            indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", "value");
        }
        indexRandom(true, indexRequestBuilders);
        assertDocCount(indexName, snapshotDocCount);

        final String leaseId = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        logger.debug("--> adding retention lease with id {} to {}", leaseId, shardId);
        client().execute(RetentionLeaseActions.Add.INSTANCE, new RetentionLeaseActions.AddRequest(shardId, leaseId, RETAIN_ALL, "test"))
            .actionGet();

        final ShardStats shardStats = Arrays.stream(client().admin().indices().prepareStats(indexName).get().getShards())
            .filter(s -> s.getShardRouting().shardId().equals(shardId))
            .findFirst()
            .get();
        final RetentionLeases retentionLeases = shardStats.getRetentionLeaseStats().retentionLeases();
        assertTrue(shardStats + ": " + retentionLeases, retentionLeases.contains(leaseId));

        final String snapshotName = "snapshot-retention-leases";
        createSnapshot(repoName, snapshotName, Collections.singletonList(indexName));

        if (randomBoolean()) {
            final int extraDocCount = iterations(10, 1000);
            logger.debug("--> indexing {} extra docs into {}", extraDocCount, indexName);
            indexRequestBuilders = new IndexRequestBuilder[extraDocCount];
            for (int i = 0; i < extraDocCount; i++) {
                indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", "value");
            }
            indexRandom(true, indexRequestBuilders);
        }

        // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
        ensureGreen();
        logger.debug("-->  close index {}", indexName);
        assertAcked(client().admin().indices().prepareClose(indexName));

        logger.debug("--> restore index {} from snapshot", indexName);
        RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(shardCount));
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));

        ensureGreen();
        assertDocCount(indexName, snapshotDocCount);

        final RetentionLeases restoredRetentionLeases = Arrays.stream(client().admin().indices().prepareStats(indexName).get().getShards())
            .filter(s -> s.getShardRouting().shardId().equals(shardId))
            .findFirst()
            .get()
            .getRetentionLeaseStats()
            .retentionLeases();
        assertFalse(restoredRetentionLeases.toString() + " has no " + leaseId, restoredRetentionLeases.contains(leaseId));
    }

    public void testAbortWaitsOnDataNode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        final String indexName = "test-index";
        createIndex(indexName);
        index(indexName, "_doc", "some_id", "foo", "bar");

        final String otherDataNode = internalCluster().startDataOnlyNode();

        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        blockAllDataNodes(repoName);
        final String snapshotName = "test-snap";
        final ActionFuture<CreateSnapshotResponse> snapshotResponse = startFullSnapshot(repoName, snapshotName);
        waitForBlock(dataNodeName, repoName, TimeValue.timeValueSeconds(30L));

        final AtomicBoolean blocked = new AtomicBoolean(true);

        final TransportService transportService = internalCluster().getInstance(TransportService.class, otherDataNode);
        transportService.addMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions finalOptions
            ) {
                if (blocked.get() && action.equals(SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME)) {
                    throw new AssertionError("Node had no assigned shard snapshots so it shouldn't send out shard state updates");
                }
            }
        });

        logger.info("--> abort snapshot");
        final ActionFuture<AcknowledgedResponse> deleteResponse = startDeleteSnapshot(repoName, snapshotName);

        awaitClusterState(
            otherDataNode,
            state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .entries()
                .stream()
                .anyMatch(entry -> entry.state() == SnapshotsInProgress.State.ABORTED)
        );

        assertFalse("delete should not be able to finish until data node is unblocked", deleteResponse.isDone());
        blocked.set(false);
        unblockAllDataNodes(repoName);
        assertAcked(deleteResponse.get());
        assertThat(snapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
    }

    public void testPartialSnapshotAllShardsMissing() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "fs");
        createIndex("some-index");
        stopNode(dataNode);
        ensureStableCluster(1);
        final CreateSnapshotResponse createSnapshotResponse = startFullSnapshot(repoName, "test-snap", true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testSnapshotDeleteRelocatingPrimaryIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        // Create index on two nodes and make sure each node has a primary by setting no replicas
        final String indexName = "test-idx";
        assertAcked(prepareCreate(indexName, 2, indexSettingsNoReplicas(between(2, 10))));
        ensureGreen(indexName);
        indexRandomDocs(indexName, 100);

        // Drop all file chunk requests so that below relocation takes forever and we're guaranteed to run the snapshot in parallel to it
        for (String nodeName : dataNodes) {
            ((MockTransportService) internalCluster().getInstance(TransportService.class, nodeName)).addSendBehavior(
                (connection, requestId, action, request, options) -> {
                    if (PeerRecoveryTargetService.Actions.FILE_CHUNK.equals(action)) {
                        return;
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );
        }

        logger.info("--> start relocations");
        allowNodes(indexName, 1);

        logger.info("--> wait for relocations to start");

        assertBusy(
            () -> assertThat(clusterAdmin().prepareHealth(indexName).execute().actionGet().getRelocatingShards(), greaterThan(0)),
            1L,
            TimeUnit.MINUTES
        );

        logger.info("--> snapshot");
        clusterAdmin().prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(false)
            .setPartial(true)
            .setIndices(indexName)
            .get();

        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> wait for snapshot to complete");
        SnapshotInfo snapshotInfo = waitForCompletion(repoName, "test-snap", TimeValue.timeValueSeconds(600));
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.PARTIAL));
        assertThat(snapshotInfo.shardFailures().size(), greaterThan(0));
        logger.info("--> done");
    }

    public void testIndexDeletionDuringSnapshotCreationInQueue() throws Exception {
        assertAcked(prepareCreate("test-idx", 1, indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs("test-idx", 100);
        createRepository("test-repo", "fs");
        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));

        logger.info("--> create snapshot to be deleted and then delete");
        createSnapshot("test-repo", "test-snap-delete", Collections.singletonList("test-idx"));
        clusterAdmin().prepareDeleteSnapshot("test-repo", "test-snap-delete").execute();

        logger.info("--> create snapshot before index deletion during above snapshot deletion");
        clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-2")
            .setWaitForCompletion(false)
            .setPartial(true)
            .setIndices("test-idx")
            .get();

        logger.info("delete index during snapshot creation");
        assertAcked(admin().indices().prepareDelete("test-idx"));

        clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap").get();
        ensureGreen("test-idx");

        // Wait for snapshot process to complete to prevent conflict with repository clean up
        assertBusy(() -> {
            SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap-2");
            assertTrue(snapshotInfo.state().completed());
            assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
        }, 1, TimeUnit.MINUTES);
    }

    private long calculateTotalFilesSize(List<Path> files) {
        return files.stream().mapToLong(f -> {
            try {
                return Files.size(f);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).sum();
    }

    private static List<Path> findRepoMetaBlobs(Path repoPath) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(repoPath.resolve("indices"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                final String fileName = file.getFileName().toString();
                if (fileName.startsWith(BlobStoreRepository.METADATA_PREFIX) && fileName.endsWith(".dat")) {
                    files.add(file);
                }
                return super.visitFile(file, attrs);
            }
        });
        return files;
    }

    private List<Path> scanSnapshotFolder(Path repoPath) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(repoPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().startsWith("__")) {
                    files.add(file);
                }
                return super.visitFile(file, attrs);
            }
        });
        return files;
    }

    public static class SnapshottableMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable";

        public SnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshottableMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshottableMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static SnapshottableMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshottableMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_AND_SNAPSHOT;
        }
    }

    public static class NonSnapshottableMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_non_snapshottable";

        public NonSnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static NonSnapshottableMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(NonSnapshottableMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static NonSnapshottableMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(NonSnapshottableMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_ONLY;
        }
    }

    public static class SnapshottableGatewayMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable_gateway";

        public SnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshottableGatewayMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshottableGatewayMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static SnapshottableGatewayMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshottableGatewayMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.SNAPSHOT, Metadata.XContentContext.GATEWAY);
        }
    }

    public static class NonSnapshottableGatewayMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_non_snapshottable_gateway";

        public NonSnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static NonSnapshottableGatewayMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(NonSnapshottableGatewayMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static NonSnapshottableGatewayMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(NonSnapshottableGatewayMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_AND_GATEWAY;
        }

    }

    public static class SnapshotableGatewayNoApiMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable_gateway_no_api";

        public SnapshotableGatewayNoApiMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshotableGatewayNoApiMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshotableGatewayNoApiMetadata::new, in);
        }

        public static SnapshotableGatewayNoApiMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshotableGatewayNoApiMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }
    }
}
