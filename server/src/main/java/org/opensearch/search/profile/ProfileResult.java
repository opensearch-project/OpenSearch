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

package org.opensearch.search.profile;

import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.InstantiatingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class is the internal representation of a profiled Query, corresponding
 * to a single node in the query tree.  It is built after the query has finished executing
 * and is merely a structured representation, rather than the entity that collects the timing
 * profile (see InternalProfiler for that)
 * <p>
 * Each InternalProfileResult has a List of InternalProfileResults, which will contain
 * "children" queries if applicable
 *
 * @opensearch.internal
 */
public final class ProfileResult implements Writeable, ToXContentObject {
    static final ParseField TYPE = new ParseField("type");
    static final ParseField DESCRIPTION = new ParseField("description");
    static final ParseField BREAKDOWN = new ParseField("breakdown");
    static final ParseField DEBUG = new ParseField("debug");
    static final ParseField NODE_TIME = new ParseField("time");
    static final ParseField MAX_SLICE_NODE_TIME = new ParseField("max_slice_time");
    static final ParseField MIN_SLICE_NODE_TIME = new ParseField("min_slice_time");
    static final ParseField AVG_SLICE_NODE_TIME = new ParseField("avg_slice_time");
    static final ParseField NODE_TIME_RAW = new ParseField("time_in_nanos");
    static final ParseField MAX_SLICE_NODE_TIME_RAW = new ParseField("max_slice_time_in_nanos");
    static final ParseField MIN_SLICE_NODE_TIME_RAW = new ParseField("min_slice_time_in_nanos");
    static final ParseField AVG_SLICE_NODE_TIME_RAW = new ParseField("avg_slice_time_in_nanos");
    static final ParseField CHILDREN = new ParseField("children");

    private final String type;
    private final String description;
    private final Map<String, Long> breakdown;
    private final Map<String, Object> debug;
    private final long nodeTime;
    private Long maxSliceNodeTime;
    private Long minSliceNodeTime;
    private Long avgSliceNodeTime;
    private final List<ProfileResult> children;

    public ProfileResult(
        String type,
        String description,
        Map<String, Long> breakdown,
        Map<String, Object> debug,
        long nodeTime,
        List<ProfileResult> children
    ) {
        this(type, description, breakdown, debug, nodeTime, children, null, null, null);
    }

    public ProfileResult(
        String type,
        String description,
        Map<String, Long> breakdown,
        Map<String, Object> debug,
        long nodeTime,
        List<ProfileResult> children,
        Long maxSliceNodeTime,
        Long minSliceNodeTime,
        Long avgSliceNodeTime
    ) {
        this.type = type;
        this.description = description;
        this.breakdown = Objects.requireNonNull(breakdown, "required breakdown argument missing");
        this.debug = debug == null ? Map.of() : debug;
        this.children = children == null ? List.of() : children;
        this.nodeTime = nodeTime;
        this.maxSliceNodeTime = maxSliceNodeTime;
        this.minSliceNodeTime = minSliceNodeTime;
        this.avgSliceNodeTime = avgSliceNodeTime;
    }

    /**
     * Read from a stream.
     */
    public ProfileResult(StreamInput in) throws IOException {
        this.type = in.readString();
        this.description = in.readString();
        this.nodeTime = in.readLong();
        breakdown = in.readMap(StreamInput::readString, StreamInput::readLong);
        debug = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        children = in.readList(ProfileResult::new);
        if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
            this.maxSliceNodeTime = in.readOptionalLong();
            this.minSliceNodeTime = in.readOptionalLong();
            this.avgSliceNodeTime = in.readOptionalLong();
        } else {
            this.maxSliceNodeTime = null;
            this.minSliceNodeTime = null;
            this.avgSliceNodeTime = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(description);
        out.writeLong(nodeTime);            // not Vlong because can be negative
        out.writeMap(breakdown, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeMap(debug, StreamOutput::writeString, StreamOutput::writeGenericValue);
        out.writeList(children);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeOptionalLong(maxSliceNodeTime);
            out.writeOptionalLong(minSliceNodeTime);
            out.writeOptionalLong(avgSliceNodeTime);
        }
    }

    /**
     * Retrieve the lucene description of this query (e.g. the "explain" text)
     */
    public String getLuceneDescription() {
        return description;
    }

    /**
     * Retrieve the name of the entry (e.g. "TermQuery" or "LongTermsAggregator")
     */
    public String getQueryName() {
        return type;
    }

    /**
     * The timing breakdown for this node.
     */
    public Map<String, Long> getTimeBreakdown() {
        return Collections.unmodifiableMap(breakdown);
    }

    /**
     * The debug information about the profiled execution.
     */
    public Map<String, Object> getDebugInfo() {
        return Collections.unmodifiableMap(debug);
    }

    /**
     * Returns the total time (inclusive of children) for this query node.
     *
     * @return  elapsed time in nanoseconds
     */
    public long getTime() {
        return nodeTime;
    }

    public Long getMaxSliceTime() {
        return maxSliceNodeTime;
    }

    public Long getMinSliceTime() {
        return minSliceNodeTime;
    }

    public Long getAvgSliceTime() {
        return avgSliceNodeTime;
    }

    /**
     * Returns a list of all profiled children queries
     */
    public List<ProfileResult> getProfiledChildren() {
        return Collections.unmodifiableList(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), type);
        builder.field(DESCRIPTION.getPreferredName(), description);
        if (builder.humanReadable()) {
            builder.field(NODE_TIME.getPreferredName(), new TimeValue(getTime(), TimeUnit.NANOSECONDS).toString());
            if (getMaxSliceTime() != null) {
                builder.field(MAX_SLICE_NODE_TIME.getPreferredName(), new TimeValue(getMaxSliceTime(), TimeUnit.NANOSECONDS).toString());
            }
            if (getMinSliceTime() != null) {
                builder.field(MIN_SLICE_NODE_TIME.getPreferredName(), new TimeValue(getMinSliceTime(), TimeUnit.NANOSECONDS).toString());
            }
            if (getAvgSliceTime() != null) {
                builder.field(AVG_SLICE_NODE_TIME.getPreferredName(), new TimeValue(getAvgSliceTime(), TimeUnit.NANOSECONDS).toString());
            }
        }
        builder.field(NODE_TIME_RAW.getPreferredName(), getTime());
        if (getMaxSliceTime() != null) {
            builder.field(MAX_SLICE_NODE_TIME_RAW.getPreferredName(), getMaxSliceTime());
        }
        if (getMinSliceTime() != null) {
            builder.field(MIN_SLICE_NODE_TIME_RAW.getPreferredName(), getMinSliceTime());
        }
        if (getAvgSliceTime() != null) {
            builder.field(AVG_SLICE_NODE_TIME_RAW.getPreferredName(), getAvgSliceTime());
        }
        createBreakdownView(builder);
        if (false == debug.isEmpty()) {
            builder.field(DEBUG.getPreferredName(), debug);
        }

        if (false == children.isEmpty()) {
            builder.startArray(CHILDREN.getPreferredName());
            for (ProfileResult child : children) {
                builder = child.toXContent(builder, params);
            }
            builder.endArray();
        }

        return builder.endObject();
    }

    private void createBreakdownView(XContentBuilder builder) throws IOException {
        Map<String, Long> modifiedBreakdown = new LinkedHashMap<>(breakdown);
        removeStartTimeFields(modifiedBreakdown);
        builder.field(BREAKDOWN.getPreferredName(), modifiedBreakdown);
    }

    static void removeStartTimeFields(Map<String, Long> modifiedBreakdown) {
        Iterator<Map.Entry<String, Long>> iterator = modifiedBreakdown.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (entry.getKey().endsWith(AbstractProfileBreakdown.TIMING_TYPE_START_TIME_SUFFIX)) {
                iterator.remove();
            }
        }
    }

    private static final InstantiatingObjectParser<ProfileResult, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ProfileResult, Void> parser = InstantiatingObjectParser.builder(
            "profile_result",
            true,
            ProfileResult.class
        );
        parser.declareString(constructorArg(), TYPE);
        parser.declareString(constructorArg(), DESCRIPTION);
        parser.declareObject(constructorArg(), (p, c) -> p.map(), BREAKDOWN);
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), DEBUG);
        parser.declareLong(constructorArg(), NODE_TIME_RAW);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> fromXContent(p), CHILDREN);
        parser.declareLong(optionalConstructorArg(), MAX_SLICE_NODE_TIME_RAW);
        parser.declareLong(optionalConstructorArg(), MIN_SLICE_NODE_TIME_RAW);
        parser.declareLong(optionalConstructorArg(), AVG_SLICE_NODE_TIME_RAW);
        PARSER = parser.build();
    }

    public static ProfileResult fromXContent(XContentParser p) throws IOException {
        return PARSER.parse(p, null);
    }
}
