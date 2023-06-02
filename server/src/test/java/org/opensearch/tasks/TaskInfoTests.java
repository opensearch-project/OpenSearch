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

package org.opensearch.tasks;

import org.opensearch.client.Requests;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class TaskInfoTests extends AbstractSerializingTestCase<TaskInfo> {

    @Override
    protected TaskInfo doParseInstance(XContentParser parser) {
        return TaskInfo.fromXContent(parser);
    }

    @Override
    protected TaskInfo createTestInstance() {
        return randomTaskInfo();
    }

    @Override
    protected Writeable.Reader<TaskInfo> instanceReader() {
        return TaskInfo::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new))
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // status, headers and resource_stats hold arbitrary content, we can't inject random fields in them
        return field -> "status".equals(field) || "headers".equals(field) || field.contains("resource_stats");
    }

    @Override
    protected TaskInfo mutateInstance(TaskInfo info) {
        switch (between(0, 11)) {
            case 0:
                TaskId taskId = new TaskId(info.getTaskId().getNodeId() + randomAlphaOfLength(5), info.getTaskId().getId());
                return new TaskInfo(
                    taskId,
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 1:
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType() + randomAlphaOfLength(5),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 2:
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction() + randomAlphaOfLength(5),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 3:
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription() + randomAlphaOfLength(5),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 4:
                Task.Status newStatus = randomValueOtherThan(info.getStatus(), TaskInfoTests::randomRawTaskStatus);
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    newStatus,
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 5:
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime() + between(1, 100),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 6:
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos() + between(1, 100),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 7:
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable() == false,
                    false,
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 8:
                TaskId parentId = new TaskId(info.getParentTaskId().getNodeId() + randomAlphaOfLength(5), info.getParentTaskId().getId());
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    parentId,
                    info.getHeaders(),
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 9:
                Map<String, String> headers = info.getHeaders();
                if (headers == null) {
                    headers = new HashMap<>(1);
                } else {
                    headers = new HashMap<>(info.getHeaders());
                }
                headers.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    headers,
                    info.getResourceStats(),
                    info.getCancellationStartTime()
                );
            case 10:
                Map<String, TaskResourceUsage> resourceUsageMap;
                if (info.getResourceStats() == null) {
                    resourceUsageMap = new HashMap<>(1);
                } else {
                    resourceUsageMap = new HashMap<>(info.getResourceStats().getResourceUsageInfo());
                }
                resourceUsageMap.put(randomAlphaOfLength(5), new TaskResourceUsage(randomNonNegativeLong(), randomNonNegativeLong()));
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    info.isCancellable(),
                    info.isCancelled(),
                    info.getParentTaskId(),
                    info.getHeaders(),
                    new TaskResourceStats(resourceUsageMap, new TaskThreadUsage(randomInt(10), randomInt(10)))
                );
            case 11:
                return new TaskInfo(
                    info.getTaskId(),
                    info.getType(),
                    info.getAction(),
                    info.getDescription(),
                    info.getStatus(),
                    info.getStartTime(),
                    info.getRunningTimeNanos(),
                    true,
                    true,
                    info.getParentTaskId(),
                    info.getHeaders(),
                    info.getResourceStats(),
                    randomNonNegativeLong()
                );
            default:
                throw new IllegalStateException();
        }
    }

    static TaskInfo randomTaskInfo() {
        return randomTaskInfo(randomBoolean());
    }

    static TaskInfo randomTaskInfo(boolean detailed) {
        TaskId taskId = randomTaskId();
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        Task.Status status = detailed ? randomRawTaskStatus() : null;
        String description = detailed ? randomAlphaOfLength(5) : null;
        long startTime = randomLong();
        long runningTimeNanos = randomLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable == true ? randomBoolean() : false;
        Long cancellationStartTime = null;
        if (cancelled) {
            cancellationStartTime = randomNonNegativeLong();
        }
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId();
        Map<String, String> headers = randomBoolean()
            ? Collections.emptyMap()
            : Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(
            taskId,
            type,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            randomResourceStats(detailed),
            cancellationStartTime
        );
    }

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(5), randomLong());
    }

    private static RawTaskStatus randomRawTaskStatus() {
        try (XContentBuilder builder = XContentBuilder.builder(Requests.INDEX_CONTENT_TYPE.xContent())) {
            builder.startObject();
            int fields = between(0, 11);
            for (int f = 0; f < fields; f++) {
                builder.field(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            builder.endObject();
            return new RawTaskStatus(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static TaskResourceStats randomResourceStats(boolean detailed) {
        return detailed ? new TaskResourceStats(new HashMap<>() {
            {
                for (int i = 0; i < randomInt(5); i++) {
                    put(randomAlphaOfLength(5), new TaskResourceUsage(randomNonNegativeLong(), randomNonNegativeLong()));
                }
            }
        }, new TaskThreadUsage(randomInt(10), randomInt(10))) : null;
    }
}
