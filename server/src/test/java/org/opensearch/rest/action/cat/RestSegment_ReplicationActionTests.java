/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.indices.segment_replication.SegmentReplicationStatsResponse;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Randomness;
import org.opensearch.common.Table;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentOpenSearchExtension;
import org.opensearch.index.Index;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.test.OpenSearchTestCase;

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

public class RestSegment_ReplicationActionTests extends OpenSearchTestCase {
    public void testSegment_ReplicationActionAction() {
        final RestCatSegmentReplicationAction action = new RestCatSegmentReplicationAction();
        final int totalShards = randomIntBetween(1, 32);
        final int successfulShards = Math.max(0, totalShards - randomIntBetween(1, 2));
        final int failedShards = totalShards - successfulShards;
        final Map<String, List<SegmentReplicationState>> shardSegmentReplicationStates = new HashMap<>();
        final List<SegmentReplicationState> segmentReplicationStates = new ArrayList<>();

        for (int i = 0; i < successfulShards; i++) {
            final SegmentReplicationState state = mock(SegmentReplicationState.class);
            final ShardRouting shardRouting = mock(ShardRouting.class);
            when(state.getShardRouting()).thenReturn(shardRouting);
            when(shardRouting.shardId()).thenReturn(new ShardId(new Index("index", "_na_"), i));
            when(state.getReplicationId()).thenReturn(randomLongBetween(0, 1000));
            final ReplicationTimer timer = mock(ReplicationTimer.class);
            final long startTime = randomLongBetween(0, new Date().getTime());
            when(timer.startTime()).thenReturn(startTime);
            final long time = randomLongBetween(1000000, 10 * 1000000);
            when(timer.time()).thenReturn(time);
            when(timer.stopTime()).thenReturn(startTime + time);
            when(state.getTimer()).thenReturn(timer);
            when(state.getStage()).thenReturn(randomFrom(SegmentReplicationState.Stage.values()));
            when(state.getSourceDescription()).thenReturn("Source");
            final DiscoveryNode targetNode = mock(DiscoveryNode.class);
            when(targetNode.getHostName()).thenReturn(randomAlphaOfLength(8));
            when(state.getTargetNode()).thenReturn(targetNode);

            ReplicationLuceneIndex index = createTestIndex();
            when(state.getIndex()).thenReturn(index);

            //

            segmentReplicationStates.add(state);
        }

        final List<SegmentReplicationState> shuffle = new ArrayList<>(segmentReplicationStates);
        Randomness.shuffle(shuffle);
        shardSegmentReplicationStates.put("index", shuffle);

        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final SegmentReplicationStatsResponse response = new SegmentReplicationStatsResponse(
            totalShards,
            successfulShards,
            failedShards,
            shardSegmentReplicationStates,
            shardFailures
        );
        final Table table = action.buildSegmentReplicationTable(null, response);

        assertNotNull(table);

        List<Table.Cell> headers = table.getHeaders();

        final List<String> expectedHeaders = Arrays.asList(
            "index",
            "shardId",
            "start_time",
            "start_time_millis",
            "stop_time",
            "stop_time_millis",
            "time",
            "stage",
            "source_description",
            "target_host",
            "target_node",
            "files_fetched",
            "files_percent",
            "bytes_fetched",
            "bytes_percent"
        );

        for (int i = 0; i < expectedHeaders.size(); i++) {
            assertThat(headers.get(i).value, equalTo(expectedHeaders.get(i)));
        }

        assertThat(table.getRows().size(), equalTo(successfulShards));

        for (int i = 0; i < successfulShards; i++) {
            final SegmentReplicationState state = segmentReplicationStates.get(i);
            final List<Object> expectedValues = Arrays.asList(
                "index",
                i,
                XContentOpenSearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().startTime()),
                state.getTimer().startTime(),
                XContentOpenSearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().stopTime()),
                state.getTimer().stopTime(),
                new TimeValue(state.getTimer().time()),
                state.getStage().name().toLowerCase(Locale.ROOT),
                state.getSourceDescription(),
                state.getTargetNode().getHostName(),
                state.getTargetNode().getName(),
                state.getIndex().recoveredFileCount(),
                percent(state.getIndex().recoveredFilesPercent()),
                state.getIndex().recoveredBytes(),
                percent(state.getIndex().recoveredBytesPercent())
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
