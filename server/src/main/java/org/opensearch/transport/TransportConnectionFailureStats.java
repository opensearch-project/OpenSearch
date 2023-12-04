/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TransportConnectionFailureStats implements ToXContentFragment, Writeable {
    // Node as key with failure count as value
    private Map<String, Integer> connectionFailureMap = ConcurrentCollections.newConcurrentMap();

    private static final TransportConnectionFailureStats INSTANCE = new TransportConnectionFailureStats();

    public TransportConnectionFailureStats() {

    }

    public TransportConnectionFailureStats(StreamInput in) throws IOException {
        connectionFailureMap = in.readMap(StreamInput::readString, StreamInput::readInt);
    }

    public static TransportConnectionFailureStats getInstance() {
        return INSTANCE;
    }

    public void updateConnectionFailureCount(String node) {
        connectionFailureMap.compute(node, (k, v) -> v == null ? 1 : v + 1);
    }

    public int getConnectionFailureCount(String node) {
        if (connectionFailureMap.containsKey(node)) {
            Integer value = connectionFailureMap.get(node);
            return value;
        }
        return 0;
    }

    public Map<String, Integer> getConnectionFailure() {
        return new ConcurrentHashMap(connectionFailureMap);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Map<String, Integer> map = connectionFailureMap;
        out.writeMap(map, StreamOutput::writeString, StreamOutput::writeInt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("transport_connection_failure");
        for (var entry : connectionFailureMap.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionFailureMap);
    }
}
