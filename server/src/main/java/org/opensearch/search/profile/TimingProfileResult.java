/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.InstantiatingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A profile result for a timing profile
 */
@PublicApi(since = "3.0.0")
public class TimingProfileResult extends AbstractProfileResult<TimingProfileResult> {
    static final ParseField NODE_TIME = new ParseField("time");
    static final ParseField MAX_SLICE_NODE_TIME = new ParseField("max_slice_time");
    static final ParseField MIN_SLICE_NODE_TIME = new ParseField("min_slice_time");
    static final ParseField AVG_SLICE_NODE_TIME = new ParseField("avg_slice_time");
    static final ParseField NODE_TIME_RAW = new ParseField("time_in_nanos");
    static final ParseField MAX_SLICE_NODE_TIME_RAW = new ParseField("max_slice_time_in_nanos");
    static final ParseField MIN_SLICE_NODE_TIME_RAW = new ParseField("min_slice_time_in_nanos");
    static final ParseField AVG_SLICE_NODE_TIME_RAW = new ParseField("avg_slice_time_in_nanos");

    private final long nodeTime;
    private Long maxSliceNodeTime;
    private Long minSliceNodeTime;
    private Long avgSliceNodeTime;

    public TimingProfileResult(
        String type,
        String description,
        Map<String, Long> breakdown,
        Map<String, Object> debug,
        long nodeTime,
        List<TimingProfileResult> children
    ) {
        this(type, description, breakdown, debug, nodeTime, children, null, null, null);
    }

    public TimingProfileResult(
        String type,
        String description,
        Map<String, Long> breakdown,
        Map<String, Object> debug,
        long nodeTime,
        List<TimingProfileResult> children,
        Long maxSliceNodeTime,
        Long minSliceNodeTime,
        Long avgSliceNodeTime
    ) {
        super(type, description, breakdown, debug, children);
        this.nodeTime = nodeTime;
        this.maxSliceNodeTime = maxSliceNodeTime;
        this.minSliceNodeTime = minSliceNodeTime;
        this.avgSliceNodeTime = avgSliceNodeTime;
    }

    /**
     * Read from a stream.
     */
    public TimingProfileResult(StreamInput in) throws IOException {
        super(in);
        children = in.readList(TimingProfileResult::new);
        this.nodeTime = in.readLong();
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
        super.writeTo(out);
        out.writeLong(nodeTime);            // not Vlong because can be negative
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeOptionalLong(maxSliceNodeTime);
            out.writeOptionalLong(minSliceNodeTime);
            out.writeOptionalLong(avgSliceNodeTime);
        }
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


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), getQueryName());
        builder.field(DESCRIPTION.getPreferredName(), getLuceneDescription());
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
        if (false == getDebugInfo().isEmpty()) {
            builder.field(DEBUG.getPreferredName(), getDebugInfo());
        }

        if (false == children.isEmpty()) {
            builder.startArray(CHILDREN.getPreferredName());
            for (TimingProfileResult child : children) {
                builder = child.toXContent(builder, params);
            }
            builder.endArray();
        }

        return builder.endObject();
    }

    private void createBreakdownView(XContentBuilder builder) throws IOException {
        Map<String, Long> modifiedBreakdown = new LinkedHashMap<>(getTimeBreakdown());
        removeStartTimeFields(modifiedBreakdown);
        builder.field(BREAKDOWN.getPreferredName(), modifiedBreakdown);
    }

    static void removeStartTimeFields(Map<String, Long> modifiedBreakdown) {
        Iterator<Map.Entry<String, Long>> iterator = modifiedBreakdown.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (entry.getKey().endsWith(AbstractTimingProfileBreakdown.TIMING_TYPE_START_TIME_SUFFIX)) {
                iterator.remove();
            }
        }
    }

    private static final InstantiatingObjectParser<TimingProfileResult, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<TimingProfileResult, Void> parser = InstantiatingObjectParser.builder(
            "profile_result",
            true,
            TimingProfileResult.class
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

    public static TimingProfileResult fromXContent(XContentParser p) throws IOException {
        return PARSER.parse(p, null);
    }
}
