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

package org.opensearch.action.admin.cluster.node.tasks;

import org.opensearch.action.search.SearchAction;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.tasks.ResourceUsageMetric;
import org.opensearch.tasks.ResourceStats;
import org.opensearch.tasks.ResourceStatsType;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.tasks.TaskInfoTests.randomResourceStats;

public class TaskTests extends OpenSearchTestCase {

    public void testTaskInfoToString() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomIntBetween(0, 100000);
        long startTime = randomNonNegativeLong();
        long runningTime = randomNonNegativeLong();
        boolean cancellable = false;
        boolean cancelled = false;
        TaskInfo taskInfo = new TaskInfo(
            new TaskId(nodeId, taskId),
            "test_type",
            "test_action",
            "test_description",
            null,
            startTime,
            runningTime,
            cancellable,
            cancelled,
            TaskId.EMPTY_TASK_ID,
            Collections.singletonMap("foo", "bar"),
            randomResourceStats(randomBoolean())
        );
        String taskInfoString = taskInfo.toString();
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(taskInfoString.getBytes(StandardCharsets.UTF_8)), true).v2();
        assertEquals(((Number) map.get("id")).longValue(), taskId);
        assertEquals(map.get("type"), "test_type");
        assertEquals(map.get("action"), "test_action");
        assertEquals(map.get("description"), "test_description");
        assertEquals(((Number) map.get("start_time_in_millis")).longValue(), startTime);
        assertEquals(((Number) map.get("running_time_in_nanos")).longValue(), runningTime);
        assertEquals(map.get("cancellable"), cancellable);
        assertEquals(map.get("cancelled"), cancelled);
        assertEquals(map.get("headers"), Collections.singletonMap("foo", "bar"));
    }

    public void testCancellableOptionWhenCancelledTrue() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomIntBetween(0, 100000);
        long startTime = randomNonNegativeLong();
        long runningTime = randomNonNegativeLong();
        long cancellationStartTime = randomNonNegativeLong();
        boolean cancellable = true;
        boolean cancelled = true;
        TaskInfo taskInfo = new TaskInfo(
            new TaskId(nodeId, taskId),
            "test_type",
            "test_action",
            "test_description",
            null,
            startTime,
            runningTime,
            cancellable,
            cancelled,
            TaskId.EMPTY_TASK_ID,
            Collections.singletonMap("foo", "bar"),
            randomResourceStats(randomBoolean()),
            cancellationStartTime
        );
        String taskInfoString = taskInfo.toString();
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(taskInfoString.getBytes(StandardCharsets.UTF_8)), true).v2();
        assertEquals(map.get("cancellable"), cancellable);
        assertEquals(map.get("cancelled"), cancelled);
        assertEquals(map.get("cancellation_time_millis"), cancellationStartTime);
    }

    public void testCancellableOptionWhenCancelledFalse() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomIntBetween(0, 100000);
        long startTime = randomNonNegativeLong();
        long runningTime = randomNonNegativeLong();
        boolean cancellable = true;
        boolean cancelled = false;
        TaskInfo taskInfo = new TaskInfo(
            new TaskId(nodeId, taskId),
            "test_type",
            "test_action",
            "test_description",
            null,
            startTime,
            runningTime,
            cancellable,
            cancelled,
            TaskId.EMPTY_TASK_ID,
            Collections.singletonMap("foo", "bar"),
            randomResourceStats(randomBoolean())
        );
        String taskInfoString = taskInfo.toString();
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(taskInfoString.getBytes(StandardCharsets.UTF_8)), true).v2();
        assertEquals(map.get("cancellable"), cancellable);
        assertEquals(map.get("cancelled"), cancelled);
    }

    public void testNonCancellableOption() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomIntBetween(0, 100000);
        long startTime = randomNonNegativeLong();
        long runningTime = randomNonNegativeLong();
        boolean cancellable = false;
        boolean cancelled = true;
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new TaskInfo(
                new TaskId(nodeId, taskId),
                "test_type",
                "test_action",
                "test_description",
                null,
                startTime,
                runningTime,
                cancellable,
                cancelled,
                TaskId.EMPTY_TASK_ID,
                Collections.singletonMap("foo", "bar"),
                randomResourceStats(randomBoolean())
            )
        );
        assertEquals(e.getMessage(), "task cannot be cancelled");
    }

    public void testTaskResourceStats() {
        final Task task = new Task(
            randomLong(),
            "transport",
            SearchAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );

        long totalMemory = 0L;
        long totalCPU = 0L;

        // reporting resource consumption events and checking total consumption values
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            long initial_memory = randomLongBetween(1, 100);
            long initial_cpu = randomLongBetween(1, 100);

            ResourceUsageMetric[] initialTaskResourceMetrics = new ResourceUsageMetric[] {
                new ResourceUsageMetric(ResourceStats.MEMORY, initial_memory),
                new ResourceUsageMetric(ResourceStats.CPU, initial_cpu) };
            task.startThreadResourceTracking(i, ResourceStatsType.WORKER_STATS, initialTaskResourceMetrics);

            long memory = initial_memory + randomLongBetween(1, 10000);
            long cpu = initial_cpu + randomLongBetween(1, 10000);

            totalMemory += memory - initial_memory;
            totalCPU += cpu - initial_cpu;

            ResourceUsageMetric[] taskResourceMetrics = new ResourceUsageMetric[] {
                new ResourceUsageMetric(ResourceStats.MEMORY, memory),
                new ResourceUsageMetric(ResourceStats.CPU, cpu) };
            task.updateThreadResourceStats(i, ResourceStatsType.WORKER_STATS, taskResourceMetrics);
            task.stopThreadResourceTracking(i, ResourceStatsType.WORKER_STATS);
        }
        assertEquals(task.getTotalResourceStats().getMemoryInBytes(), totalMemory);
        assertEquals(task.getTotalResourceStats().getCpuTimeInNanos(), totalCPU);
        // Basic sanity testing that min < average < max < total
        assertTrue(task.getMinResourceStats().getMemoryInBytes() < task.getAverageResourceStats().getMemoryInBytes());
        assertTrue(task.getAverageResourceStats().getMemoryInBytes() < task.getMaxResourceStats().getMemoryInBytes());
        assertTrue(task.getMaxResourceStats().getMemoryInBytes() < task.getTotalResourceStats().getMemoryInBytes());

        // updating should throw an IllegalStateException when active entry is not present.
        try {
            task.updateThreadResourceStats(randomInt(), ResourceStatsType.WORKER_STATS);
            fail("update should not be successful as active entry is not present!");
        } catch (IllegalStateException e) {
            // pass
        }

        // re-adding a thread entry that is already present, should throw an exception
        int threadId = randomInt();
        task.startThreadResourceTracking(threadId, ResourceStatsType.WORKER_STATS, new ResourceUsageMetric(ResourceStats.MEMORY, 100));
        try {
            task.startThreadResourceTracking(threadId, ResourceStatsType.WORKER_STATS);
            fail("add/start should not be successful as active entry is already present!");
        } catch (IllegalStateException e) {
            // pass
        }

        // existing active entry is present only for memory, update cannot be called with cpu values.
        try {
            task.updateThreadResourceStats(threadId, ResourceStatsType.WORKER_STATS, new ResourceUsageMetric(ResourceStats.CPU, 200));
            fail("update should not be successful as entry for CPU is not present!");
        } catch (IllegalStateException e) {
            // pass
        }
    }
}
