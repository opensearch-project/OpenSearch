/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.action.search.SearchType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * The Latency record stored in the Query Insight Framework
 *
 * @opensearch.internal
 */
public final class SearchQueryLatencyRecord extends SearchQueryRecord<Long> {

    private static final String PHASE_LATENCY_MAP = "phaseLatencyMap";
    private static final String TOOK = "tookInNs";

    // latency info for each search phase
    private final Map<String, Long> phaseLatencyMap;

    public SearchQueryLatencyRecord(final StreamInput in) throws IOException {
        super(in);
        this.phaseLatencyMap = in.readMap(StreamInput::readString, StreamInput::readLong);
        this.setValue(in.readLong());
    }

    public SearchQueryLatencyRecord(
        final Long timestamp,
        final SearchType searchType,
        final String source,
        final int totalShards,
        final String[] indices,
        final Map<String, Object> propertyMap,
        final Map<String, Long> phaseLatencyMap,
        final Long tookInNanos
    ) {
        super(timestamp, searchType, source, totalShards, indices, propertyMap, tookInNanos);
        this.phaseLatencyMap = phaseLatencyMap;
    }

    public Map<String, Long> getPhaseLatencyMap() {
        return phaseLatencyMap;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.toXContent(builder, params);
        builder.field(PHASE_LATENCY_MAP, this.getPhaseLatencyMap());
        builder.field(TOOK, this.getValue());
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(phaseLatencyMap, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeLong(getValue());
    }

    public boolean equals(SearchQueryLatencyRecord other) {
        if (!super.equals(other)) {
            return false;
        }
        for (String key : phaseLatencyMap.keySet()) {
            if (!other.getPhaseLatencyMap().containsKey(key)) {
                return false;
            }
            if (!phaseLatencyMap.get(key).equals(other.getPhaseLatencyMap().get(key))) {
                return false;
            }
        }
        return true;
    }
}
