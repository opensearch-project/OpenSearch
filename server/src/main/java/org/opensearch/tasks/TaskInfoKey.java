/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TaskInfoKey {

    private final long taskId;
    private final List<String> indices;
    private final ShardId shardId;
    private final String action;

    public TaskInfoKey(long taskId) {
        this(taskId, new ArrayList<>(), null, null);
    }

    public TaskInfoKey(long taskId, List<String> indices, ShardId shardId, String action) {
        this.taskId = taskId;
        this.indices = indices;
        this.shardId = shardId;
        this.action = action;
    }

    public long getTaskId() {
        return taskId;
    }

    public List<String> getIndices() {
        return indices;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskInfoKey that = (TaskInfoKey) o;
        return Objects.equals(taskId, that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }

}
