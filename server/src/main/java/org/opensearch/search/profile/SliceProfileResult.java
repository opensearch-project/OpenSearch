/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.InstantiatingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Profiling result for a single slice (the unit of parallelism in concurrent search). Holds the
 * timing breakdown attributed to that slice alone, the slice's node time, and the
 * {@link PartitionInfo partitions} (segment ordinal plus doc-id range) the slice searched.
 *
 * <p>This mirrors the per-slice / per-partition structure of Lucene's sandbox query profiler
 * ({@code SliceProfilerResult} / {@code AggregatedQueryLeafProfilerResult}): a whole segment appears
 * as a partition whose doc-id range spans the segment, while under intra-segment search a segment is
 * split into partitions each covering a doc-id sub-range (and the same segment ordinal may then
 * appear across multiple slices).
 *
 * <p>Emitted additively, alongside the existing {@code max_/min_/avg_} slice aggregates on
 * {@link ProfileResult}: the per-slice breakdowns are the raw detail from which those aggregates are
 * derived, exposed so consumers can inspect individual slices (e.g. to spot skew).
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public class SliceProfileResult implements Writeable, ToXContentObject {

    static final ParseField SLICE_ID = new ParseField("slice_id");
    static final ParseField SLICE_TIME = new ParseField("slice_time");
    static final ParseField SLICE_TIME_RAW = new ParseField("slice_time_in_nanos");
    static final ParseField PARTITIONS = new ParseField("partitions");
    static final ParseField BREAKDOWN = new ParseField("breakdown");

    /**
     * Identity of a leaf partition searched within a slice: the segment ordinal and the doc-id range
     * {@code [minDocId, maxDocId)}. A whole-segment partition uses {@code minDocId == 0} and
     * {@code maxDocId == Integer.MAX_VALUE} (the "entire segment" sentinel used by Lucene's
     * {@code LeafReaderContextPartition}).
     */
    @PublicApi(since = "3.8.0")
    public static class PartitionInfo implements Writeable, ToXContentObject {
        static final ParseField SEGMENT_ORD = new ParseField("segment_ord");
        static final ParseField DOC_RANGE = new ParseField("doc_range");

        private final int segmentOrd;
        private final int minDocId;
        private final int maxDocId;

        public PartitionInfo(int segmentOrd, int minDocId, int maxDocId) {
            this.segmentOrd = segmentOrd;
            this.minDocId = minDocId;
            this.maxDocId = maxDocId;
        }

        public PartitionInfo(StreamInput in) throws IOException {
            this.segmentOrd = in.readVInt();
            this.minDocId = in.readInt();
            this.maxDocId = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(segmentOrd);
            out.writeInt(minDocId);
            out.writeInt(maxDocId);
        }

        public int getSegmentOrd() {
            return segmentOrd;
        }

        public int getMinDocId() {
            return minDocId;
        }

        public int getMaxDocId() {
            return maxDocId;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SEGMENT_ORD.getPreferredName(), segmentOrd);
            builder.array(DOC_RANGE.getPreferredName(), minDocId, maxDocId);
            return builder.endObject();
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<PartitionInfo, Void> PARSER = new ConstructingObjectParser<>(
            "partition_info",
            true,
            args -> {
                final List<Integer> docRange = (List<Integer>) args[1];
                return new PartitionInfo((int) args[0], docRange.get(0), docRange.get(1));
            }
        );
        static {
            PARSER.declareInt(constructorArg(), SEGMENT_ORD);
            PARSER.declareIntArray(constructorArg(), DOC_RANGE);
        }

        public static PartitionInfo fromXContent(XContentParser p) throws IOException {
            return PARSER.parse(p, null);
        }
    }

    private final int sliceId;
    private final long sliceNodeTime;
    private final List<PartitionInfo> partitions;
    private final Map<String, Long> breakdown;

    public SliceProfileResult(int sliceId, long sliceNodeTime, List<PartitionInfo> partitions, Map<String, Long> breakdown) {
        this.sliceId = sliceId;
        this.sliceNodeTime = sliceNodeTime;
        this.partitions = Collections.unmodifiableList(Objects.requireNonNull(partitions));
        this.breakdown = Collections.unmodifiableMap(Objects.requireNonNull(breakdown));
    }

    public SliceProfileResult(StreamInput in) throws IOException {
        this.sliceId = in.readVInt();
        this.sliceNodeTime = in.readLong();
        this.partitions = in.readList(PartitionInfo::new);
        this.breakdown = in.readMap(StreamInput::readString, StreamInput::readLong);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(sliceId);
        out.writeLong(sliceNodeTime);
        out.writeList(partitions);
        out.writeMap(breakdown, StreamOutput::writeString, StreamOutput::writeLong);
    }

    /** Stable identifier of this slice within the query node. */
    public int getSliceId() {
        return sliceId;
    }

    /** Wall-clock span of this slice for this query node, in nanoseconds. */
    public long getSliceNodeTime() {
        return sliceNodeTime;
    }

    /** The partitions (segment ordinal + doc-id range) searched within this slice. */
    public List<PartitionInfo> getPartitions() {
        return partitions;
    }

    /** The timing breakdown attributed to this slice. */
    public Map<String, Long> getBreakdown() {
        return breakdown;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SLICE_ID.getPreferredName(), sliceId);
        if (builder.humanReadable()) {
            builder.field(SLICE_TIME.getPreferredName(), new TimeValue(sliceNodeTime, TimeUnit.NANOSECONDS).toString());
        }
        builder.field(SLICE_TIME_RAW.getPreferredName(), sliceNodeTime);
        builder.startArray(PARTITIONS.getPreferredName());
        for (PartitionInfo partition : partitions) {
            partition.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(BREAKDOWN.getPreferredName(), breakdown);
        return builder.endObject();
    }

    private static final InstantiatingObjectParser<SliceProfileResult, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<SliceProfileResult, Void> parser = InstantiatingObjectParser.builder(
            "slice_profile_result",
            true,
            SliceProfileResult.class
        );
        parser.declareInt(constructorArg(), SLICE_ID);
        parser.declareLong(constructorArg(), SLICE_TIME_RAW);
        parser.declareObjectArray(constructorArg(), (p, c) -> PartitionInfo.fromXContent(p), PARTITIONS);
        parser.declareObject(constructorArg(), (p, c) -> {
            final Map<String, Object> raw = p.map();
            final Map<String, Long> breakdown = new HashMap<>(raw.size());
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                breakdown.put(entry.getKey(), ((Number) entry.getValue()).longValue());
            }
            return breakdown;
        }, BREAKDOWN);
        PARSER = parser.build();
    }

    public static SliceProfileResult fromXContent(XContentParser p) throws IOException {
        return PARSER.parse(p, null);
    }
}
