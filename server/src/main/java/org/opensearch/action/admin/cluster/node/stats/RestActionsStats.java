/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rest actions responses stats
 *
 * @opensearch.internal
 */
public class RestActionsStats implements Writeable, ToXContentFragment {
    /**
     * Map that contains mapping of requested actions and their response status and count
     * Ex: {"document_index_action"={200=10, 500=2}, "document_get_action"={200=20, 8}}
     */
    private Map<String, Map<Integer, AtomicLong>> restActionsStatusCount;
    /**
     * Aggregation of all response status count for requested rest actions (Aggregation of above map)
     * Ex: {200=30, 500=10}
     */
    private Map<Integer, AtomicLong> restActionsStatusTotalCount;

    public RestActionsStats(StreamInput in) throws IOException {
        int totalCount = in.readInt();
        restActionsStatusTotalCount = new TreeMap<>();
        for (int i = 0; i < totalCount; i++) {
            restActionsStatusTotalCount.put(in.readInt(), new AtomicLong(in.readLong()));
        }

        int actionsCount = in.readInt();
        restActionsStatusCount = new ConcurrentHashMap<>();
        for (int i = 0; i < actionsCount; i++) {
            String actionName = in.readString();
            int statusCountSize = in.readInt();
            Map<Integer, AtomicLong> statusCount = new TreeMap<>();
            for (int j = 0; j < statusCountSize; j++) {
                statusCount.put(in.readInt(), new AtomicLong(in.readLong()));
            }
            restActionsStatusCount.put(actionName, statusCount);
        }
    }

    public RestActionsStats(
        Map<String, Map<Integer, AtomicLong>> restActionsStatusCount,
        Map<Integer, AtomicLong> restActionsStatusTotalCount
    ) {
        this.restActionsStatusCount = restActionsStatusCount;
        this.restActionsStatusTotalCount = restActionsStatusTotalCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("rest_actions");
        if (restActionsStatusTotalCount.size() > 0) {
            builder.startObject("all");
            for (Map.Entry<Integer, AtomicLong> entry : restActionsStatusTotalCount.entrySet()) {
                builder.field(String.valueOf(entry.getKey()), entry.getValue().longValue());
            }
            builder.endObject();
        }
        for (Map.Entry<String, Map<Integer, AtomicLong>> entry : restActionsStatusCount.entrySet()) {
            builder.startObject(entry.getKey());
            for (Map.Entry<Integer, AtomicLong> innerEntry : entry.getValue().entrySet()) {
                builder.field(String.valueOf(innerEntry.getKey()), innerEntry.getValue().longValue());
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(restActionsStatusTotalCount.size());
        for (Map.Entry<Integer, AtomicLong> entry : restActionsStatusTotalCount.entrySet()) {
            out.writeInt(entry.getKey().intValue());
            out.writeLong(entry.getValue().longValue());
        }
        out.writeInt(restActionsStatusCount.size());
        for (Map.Entry<String, Map<Integer, AtomicLong>> entry : restActionsStatusCount.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Map.Entry<Integer, AtomicLong> innerEntry : entry.getValue().entrySet()) {
                out.writeInt(innerEntry.getKey().intValue());
                out.writeLong(innerEntry.getValue().longValue());
            }
        }
    }

    // Added for jUnits
    public Map<String, Map<Integer, AtomicLong>> getRestActionsStatusCount() {
        return Collections.unmodifiableMap(restActionsStatusCount);
    }

    public Map<Integer, AtomicLong> getRestActionsStatusTotalCount() {
        return Collections.unmodifiableMap(restActionsStatusTotalCount);
    }
}
