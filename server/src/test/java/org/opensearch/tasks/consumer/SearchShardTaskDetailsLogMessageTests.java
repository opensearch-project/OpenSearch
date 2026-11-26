/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.logging.OpenSearchLogMessage;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class SearchShardTaskDetailsLogMessageTests extends OpenSearchSingleNodeTestCase {
    public void testTaskDetailsLogHasJsonFields() {
        SearchShardTask task = new SearchShardTask(
            0,
            "n/a",
            "n/a",
            "test",
            null,
            Collections.singletonMap(Task.X_OPAQUE_ID, "my_id"),
            () -> "test_metadata"
        );
        SearchShardTaskDetailsLogMessage p = new SearchShardTaskDetailsLogMessage(task);

        assertThat(p.getValueFor("taskId"), equalTo("0"));
        assertThat(p.getValueFor("type"), equalTo("n/a"));
        assertThat(p.getValueFor("action"), equalTo("n/a"));
        assertThat(p.getValueFor("description"), equalTo("test"));
        assertThat(p.getValueFor("parentTaskId"), equalTo(null));
        // when no resource information present
        assertThat(p.getValueFor("resource_stats"), equalTo("{}"));
        assertThat(p.getValueFor("metadata"), equalTo("test_metadata"));

        task.startThreadResourceTracking(
            0,
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(ResourceStats.MEMORY, 0L),
            new ResourceUsageMetric(ResourceStats.CPU, 0L)
        );
        task.updateThreadResourceStats(
            0,
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(ResourceStats.MEMORY, 100),
            new ResourceUsageMetric(ResourceStats.CPU, 100)
        );
        assertThat(
            p.getValueFor("resource_stats"),
            equalTo("{0=[{cpu_time_in_nanos=100, memory_in_bytes=100}, stats_type=worker_stats, is_active=true, threadId=0]}")
        );
    }

    public void testMetadataWithPlaceholdersIsNotParsedAsLogPattern() {
        SearchShardTask task = new SearchShardTask(
            0,
            "n/a",
            "n/a",
            "test",
            null,
            Collections.singletonMap(Task.X_OPAQUE_ID, "my_id"),
            () -> "{\"query\":{\"term\":{\"field\":\"a{}b{}c\"}}}"
        );
        SearchShardTaskDetailsLogMessage p = new SearchShardTaskDetailsLogMessage(task);

        assertThat(formatWithoutStatusWarnings(p), containsString("a{}b{}c"));
    }

    public void testTaskDetailsLogWithJsonMetadata() {
        String jsonMetadata = "{\"query\":{\"match_all\":{}},\"size\":10}";
        SearchShardTask task = new SearchShardTask(
            1,
            "transport",
            "indices:data/read/search[phase/query]",
            "test",
            null,
            Collections.singletonMap(Task.X_OPAQUE_ID, "my_id"),
            () -> jsonMetadata
        );
        SearchShardTaskDetailsLogMessage p = new SearchShardTaskDetailsLogMessage(task);

        // Verify that metadata with JSON content is stored correctly
        assertThat(p.getValueFor("metadata"), equalTo(jsonMetadata));
    }

    private static String formatWithoutStatusWarnings(OpenSearchLogMessage message) {
        List<StatusData> statusData = new CopyOnWriteArrayList<>();
        StatusListener listener = new StatusConsoleListener(Level.WARN) {
            @Override
            public void log(StatusData data) {
                statusData.add(data);
            }
        };
        StatusLogger.getLogger().registerListener(listener);
        try {
            String formatted = message.getFormattedMessage();
            assertThat(statusData.stream().map(StatusData::getFormattedStatus).collect(Collectors.toList()), empty());
            return formatted;
        } finally {
            StatusLogger.getLogger().removeListener(listener);
        }
    }
}
