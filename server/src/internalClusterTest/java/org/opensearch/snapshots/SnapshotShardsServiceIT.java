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

import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.plugins.Plugin;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotShardsServiceIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, MockTransportService.TestPlugin.class);
    }

    public void testRetryPostingSnapshotStatusMessages() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository("test-repo", "mock");

        final int shards = between(1, 10);
        assertAcked(prepareCreate("test-index", 0, indexSettingsNoReplicas(shards)));
        ensureGreen();
        indexRandomDocs("test-index", scaledRandomIntBetween(50, 100));

        logger.info("--> blocking repository");
        String blockedNode = blockNodeWithIndex("test-repo", "test-index");
        dataNodeClient().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-index")
            .get();
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        final SnapshotId snapshotId = getSnapshot("test-repo", "test-snap").snapshotId();

        logger.info("--> start disrupting cluster");
        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.NetworkDelay.random(random()));
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> unblocking repository");
        unblockNode("test-repo", blockedNode);

        // Retrieve snapshot status from the data node.
        SnapshotShardsService snapshotShardsService = internalCluster().getInstance(SnapshotShardsService.class, blockedNode);
        assertBusy(() -> {
            final Snapshot snapshot = new Snapshot("test-repo", snapshotId);
            List<IndexShardSnapshotStatus.Stage> stages = snapshotShardsService.currentSnapshotShards(snapshot)
                .values()
                .stream()
                .map(status -> status.asCopy().getStage())
                .collect(Collectors.toList());
            assertThat(stages, hasSize(shards));
            assertThat(stages, everyItem(equalTo(IndexShardSnapshotStatus.Stage.DONE)));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stop disrupting cluster");
        networkDisruption.stopDisrupting();
        internalCluster().clearDisruptionScheme(true);

        assertBusy(() -> {
            SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
            logger.info("Snapshot status [{}], successfulShards [{}]", snapshotInfo.state(), snapshotInfo.successfulShards());
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
            assertThat(snapshotInfo.successfulShards(), equalTo(shards));
        }, 30L, TimeUnit.SECONDS);
    }
}
