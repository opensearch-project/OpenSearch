/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.search.aggregations.support;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;

/**
 * Data describing an agg
*
* @opensearch.internal
*/
public class ProtobufAggregationInfo implements ProtobufReportingService.ProtobufInfo {

    private final Map<String, Set<String>> aggs;

    ProtobufAggregationInfo(Map<String, Map<String, LongAdder>> aggs) {
        // we use a treemap/treeset here to have a test-able / predictable order
        Map<String, Set<String>> aggsMap = new TreeMap<>();
        aggs.forEach((s, m) -> aggsMap.put(s, Collections.unmodifiableSet(new TreeSet<>(m.keySet()))));
        this.aggs = Collections.unmodifiableMap(aggsMap);
    }

    /**
     * Read from a stream.
    */
    public ProtobufAggregationInfo(CodedInputStream in) throws IOException {
        aggs = new TreeMap<>();
        final int size = in.readInt32();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            final int keys = in.readInt32();
            final Set<String> types = new TreeSet<>();
            for (int j = 0; j < keys; j++) {
                types.add(in.readString());
            }
            aggs.put(key, types);
        }
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(aggs.size());
        for (Map.Entry<String, Set<String>> e : aggs.entrySet()) {
            out.writeStringNoTag(e.getKey());
            out.writeInt32NoTag(e.getValue().size());
            for (String type : e.getValue()) {
                out.writeStringNoTag(type);
            }
        }
    }

    public Map<String, Set<String>> getAggregations() {
        return aggs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("aggregations");
        for (Map.Entry<String, Set<String>> e : aggs.entrySet()) {
            builder.startObject(e.getKey());
            builder.startArray("types");
            for (String s : e.getValue()) {
                builder.value(s);
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
