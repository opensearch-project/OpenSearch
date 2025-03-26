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

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.Randomness;
import org.opensearch.common.Table;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentOpenSearchExtension;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestRecoveryActionTests extends OpenSearchTestCase {

    public void testRestRecoveryAction() {
        final RestCatRecoveryAction action = new RestCatRecoveryAction();
        final int totalShards = randomIntBetween(1, 32);
        final int successfulShards = Math.max(0, totalShards - randomIntBetween(1, 2));
        final int failedShards = totalShards - successfulShards;
        final Map<String, List<RecoveryState>> shardRecoveryStates = new HashMap<>();
        final List<RecoveryState> recoveryStates = new ArrayList<>();

        for (int i = 0; i < successfulShards; i++) {
            final RecoveryState state = mock(RecoveryState.class);
            when(state.getShardId()).thenReturn(new ShardId(new Index("index", "_na_"), i));
            final ReplicationTimer timer = mock(ReplicationTimer.class);
            final long startTime = randomLongBetween(0, new Date().getTime());
            when(timer.startTime()).thenReturn(startTime);
            final long time = randomLongBetween(1000000, 10 * 1000000);
            when(timer.time()).thenReturn(time);
            when(timer.stopTime()).thenReturn(startTime + time);
            when(state.getTimer()).thenReturn(timer);
            when(state.getRecoverySource()).thenReturn(TestShardRouting.randomRecoverySource());
            when(state.getStage()).thenReturn(randomFrom(RecoveryState.Stage.values()));
            final DiscoveryNode sourceNode = randomBoolean() ? mock(DiscoveryNode.class) : null;
            if (sourceNode != null) {
                when(sourceNode.getHostName()).thenReturn(randomAlphaOfLength(8));
            }
            when(state.getSourceNode()).thenReturn(sourceNode);
            final DiscoveryNode targetNode = mock(DiscoveryNode.class);
            when(targetNode.getHostName()).thenReturn(randomAlphaOfLength(8));
            when(state.getTargetNode()).thenReturn(targetNode);

            ReplicationLuceneIndex index = createTestIndex();
            when(state.getIndex()).thenReturn(index);

            final RecoveryState.Translog translog = mock(RecoveryState.Translog.class);
            final int translogOps = randomIntBetween(0, 1 << 18);
            when(translog.totalOperations()).thenReturn(translogOps);
            final int translogOpsRecovered = randomIntBetween(0, translogOps);
            when(translog.recoveredOperations()).thenReturn(translogOpsRecovered);
            when(translog.recoveredPercent()).thenReturn(translogOps == 0 ? 100f : (100f * translogOpsRecovered / translogOps));
            when(state.getTranslog()).thenReturn(translog);

            recoveryStates.add(state);
        }

        final List<RecoveryState> shuffle = new ArrayList<>(recoveryStates);
        Randomness.shuffle(shuffle);
        shardRecoveryStates.put("index", shuffle);

        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final RecoveryResponse response = new RecoveryResponse(
            totalShards,
            successfulShards,
            failedShards,
            shardRecoveryStates,
            shardFailures
        );
        final Table table = action.buildRecoveryTable(null, response);

        assertNotNull(table);

        List<Table.Cell> headers = table.getHeaders();

        final List<String> expectedHeaders = Arrays.asList(
            "index",
            "shard",
            "start_time",
            "start_time_millis",
            "stop_time",
            "stop_time_millis",
            "time",
            "type",
            "stage",
            "source_host",
            "source_node",
            "target_host",
            "target_node",
            "repository",
            "snapshot",
            "files",
            "files_recovered",
            "files_percent",
            "files_total",
            "bytes",
            "bytes_recovered",
            "bytes_percent",
            "bytes_total",
            "translog_ops",
            "translog_ops_recovered",
            "translog_ops_percent"
        );

        for (int i = 0; i < expectedHeaders.size(); i++) {
            assertThat(headers.get(i).value, equalTo(expectedHeaders.get(i)));
        }

        assertThat(table.getRows().size(), equalTo(successfulShards));

        for (int i = 0; i < successfulShards; i++) {
            final RecoveryState state = recoveryStates.get(i);
            final List<Object> expectedValues = Arrays.asList(
                "index",
                i,
                XContentOpenSearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(state.getTimer().startTime())),
                state.getTimer().startTime(),
                XContentOpenSearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(state.getTimer().stopTime())),
                state.getTimer().stopTime(),
                new TimeValue(state.getTimer().time()),
                state.getRecoverySource().getType().name().toLowerCase(Locale.ROOT),
                state.getStage().name().toLowerCase(Locale.ROOT),
                state.getSourceNode() == null ? "n/a" : state.getSourceNode().getHostName(),
                state.getSourceNode() == null ? "n/a" : state.getSourceNode().getName(),
                state.getTargetNode().getHostName(),
                state.getTargetNode().getName(),
                state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT
                    ? "n/a"
                    : ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getRepository(),
                state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT
                    ? "n/a"
                    : ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getSnapshotId().getName(),
                state.getIndex().totalRecoverFiles(),
                state.getIndex().recoveredFileCount(),
                percent(state.getIndex().recoveredFilesPercent()),
                state.getIndex().totalFileCount(),
                new ByteSizeValue(state.getIndex().totalRecoverBytes()),
                new ByteSizeValue(state.getIndex().recoveredBytes()),
                percent(state.getIndex().recoveredBytesPercent()),
                new ByteSizeValue(state.getIndex().totalBytes()),
                state.getTranslog().totalOperations(),
                state.getTranslog().recoveredOperations(),
                percent(state.getTranslog().recoveredPercent())
            );

            final List<Table.Cell> cells = table.getRows().get(i);
            for (int j = 0; j < expectedValues.size(); j++) {
                assertThat(cells.get(j).value, equalTo(expectedValues.get(j)));
            }
        }
    }

    private ReplicationLuceneIndex createTestIndex() {
        ReplicationLuceneIndex index = new ReplicationLuceneIndex();
        final int filesToRecoverCount = randomIntBetween(1, 64);
        final int recoveredFilesCount = randomIntBetween(0, filesToRecoverCount);
        addTestFileMetadata(index, 0, recoveredFilesCount, false, true);
        addTestFileMetadata(index, recoveredFilesCount, filesToRecoverCount, false, false);

        final int totalFilesCount = randomIntBetween(filesToRecoverCount, 2 * filesToRecoverCount);
        addTestFileMetadata(index, filesToRecoverCount, totalFilesCount, true, false);
        return index;
    }

    private void addTestFileMetadata(ReplicationLuceneIndex index, int startIndex, int endIndex, boolean reused, boolean isFullyRecovered) {
        for (int i = startIndex; i < endIndex; i++) {
            final int completeFileSize = randomIntBetween(1, 1024);
            index.addFileDetail(String.valueOf(i), completeFileSize, reused);

            if (!reused) {
                final int recoveredFileSize;
                if (isFullyRecovered) {
                    recoveredFileSize = completeFileSize;

                } else {
                    recoveredFileSize = randomIntBetween(0, completeFileSize);
                }
                index.addRecoveredBytesToFile(String.valueOf(i), recoveredFileSize);
            }
        }
    }

    private static String percent(float percent) {
        return String.format(Locale.ROOT, "%1.1f%%", percent);
    }

}
