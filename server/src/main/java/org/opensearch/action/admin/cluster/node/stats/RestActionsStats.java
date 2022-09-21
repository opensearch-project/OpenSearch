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
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rest actions responses stats
 *
 * @opensearch.internal
 */
public class RestActionsStats implements Writeable, ToXContentFragment {
    private Map<String, Map<Integer, Integer>> restActionsStatusCount;
    private Map<Integer, Integer> restActionsStatusTotalCount;
    public RestActionsStats(StreamInput in) throws IOException {
        int totalCount = in.readInt();
        restActionsStatusTotalCount = new TreeMap<>();
        for (int i = 0; i < totalCount; i++) {
            restActionsStatusTotalCount.put(in.readInt(), in.readInt());
        }

        int actionsCount = in.readInt();
        restActionsStatusCount = new ConcurrentHashMap<>();
        for (int i = 0; i < actionsCount; i++) {
            String actionName = in.readString();
            int statusCount = in.readInt();
            Map<Integer, Integer> mapStatusCount = new TreeMap<>();
            for (int j = 0; j < statusCount; j++) {
                mapStatusCount.put(in.readInt(), in.readInt());
            }
            restActionsStatusCount.put(actionName, mapStatusCount);
        }
    }

    public RestActionsStats(Map<String, Map<Integer, Integer>> restActionsStatusCount, Map<Integer, Integer> restActionsStatusTotalCount) {
        this.restActionsStatusCount = restActionsStatusCount;
        this.restActionsStatusTotalCount = restActionsStatusTotalCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("rest_actions");
        builder.startObject("all");
        for (Map.Entry<Integer, Integer> entry : restActionsStatusTotalCount.entrySet()) {
            builder.field(String.valueOf(entry.getKey()), entry.getValue());
        }
        builder.endObject();
        for (Map.Entry<String, Map<Integer, Integer>> entry : restActionsStatusCount.entrySet()) {
            builder.startObject(entry.getKey());
            for (Map.Entry<Integer, Integer> innerEntry : entry.getValue().entrySet()) {
                builder.field(String.valueOf(innerEntry.getKey()), innerEntry.getValue());
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(restActionsStatusTotalCount.size());
        for (Map.Entry<Integer, Integer> entry : restActionsStatusTotalCount.entrySet()) {
            out.writeInt(entry.getKey().intValue());
            out.writeInt(entry.getValue().intValue());
        }
        out.writeInt(restActionsStatusCount.size());
        for (Map.Entry<String, Map<Integer, Integer>> entry : restActionsStatusCount.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Map.Entry<Integer, Integer> innerEntry : entry.getValue().entrySet()) {
                out.writeInt(innerEntry.getKey().intValue());
                out.writeInt(innerEntry.getValue().intValue());
            }
        }
    }

    //Added for jUnits
    public Map<String, Map<Integer, Integer>> getRestActionsStatusCount(){
        return Collections.unmodifiableMap(restActionsStatusCount);
    }
    public Map<Integer, Integer> getRestActionsStatusTotalCount(){
        return Collections.unmodifiableMap(restActionsStatusTotalCount);
    }
}
