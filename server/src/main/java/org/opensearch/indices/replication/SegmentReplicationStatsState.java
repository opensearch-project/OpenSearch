/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;


import java.io.IOException;


public class SegmentReplicationStatsState implements ToXContentFragment, Writeable {

    private SegmentReplicationState segmentReplicationState;

    public SegmentReplicationStatsState(ShardRouting shardRouting) {
        this.shardRouting = shardRouting;
    }

    public long getNumberOfReplicationEvents() {
        return numberOfReplicationEvents;
    }

    public void incrementCountOfSegmentReplicationEvents() {
        numberOfReplicationEvents++;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    private ShardRouting shardRouting;

    private long numberOfReplicationEvents;

    public SegmentReplicationStatsState(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        numberOfReplicationEvents = in.readLong();
        segmentReplicationState =new SegmentReplicationState(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeLong(numberOfReplicationEvents);
        segmentReplicationState.writeTo(out);
    }

    protected void setSegmentReplicationState(SegmentReplicationState segmentReplicationState) {
        this.segmentReplicationState = segmentReplicationState;
    }

    public SegmentReplicationState getSegmentReplicationState(){
        return segmentReplicationState;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {


        builder.field(Fields.ID, shardRouting.shardId().id());
        builder.field(Fields.STAGE, segmentReplicationState.getStage());
        builder.field(Fields.COUNT_OF_SEGMENT_REPLICATION_EVENTS, numberOfReplicationEvents);
        builder.timeField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, segmentReplicationState.getTimer().startTime());
        if (segmentReplicationState.getTimer().stopTime() > 0) {
            builder.timeField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, segmentReplicationState.getTimer().stopTime());
        }
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(segmentReplicationState.getTimer().time()));
        builder.field(Fields.INIT_STAGE, segmentReplicationState.getTimingData().get(SegmentReplicationState.Stage.INIT.toString()));
        builder.field(Fields.REPLICATING_STAGE, segmentReplicationState.getTimingData().get(SegmentReplicationState.Stage.REPLICATING.toString()));
        builder.field(Fields.GET_CHECKPOINT_INFO_STAGE, segmentReplicationState.getTimingData().get(SegmentReplicationState.Stage.GET_CHECKPOINT_INFO.toString()));
        builder.field(Fields.FILE_DIFF_STAGE, segmentReplicationState.getTimingData().get(SegmentReplicationState.Stage.FILE_DIFF.toString()));
        builder.field(Fields.GET_FILES_STAGE, segmentReplicationState.getTimingData().get(SegmentReplicationState.Stage.GET_FILES.toString()));
        builder.field(Fields.FINALIZE_REPLICATION_STAGE, segmentReplicationState.getTimingData().get(SegmentReplicationState.Stage.FINALIZE_REPLICATION.toString()));
        return builder;
    }

    /**
     * Fields used in the segment replication stats state
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String ID = "id";
        static final String COUNT_OF_SEGMENT_REPLICATION_EVENTS = "count_of_segment_replication_events";
        static final String STAGE = "stage";
        static final String START_TIME = "start_time";
        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String STOP_TIME = "stop_time";
        static final String STOP_TIME_IN_MILLIS = "stop_time_in_millis";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String SOURCE = "source";
        static final String HOST = "host";
        static final String TRANSPORT_ADDRESS = "transport_address";
        static final String IP = "ip";
        static final String NAME = "name";
        static final String TARGET = "target";
        static final String INIT_STAGE = "init_stage";
        static final String REPLICATING_STAGE = "replicating_stage";
        static final String GET_CHECKPOINT_INFO_STAGE = "get_checkpoint_info_stage";
        static final String FILE_DIFF_STAGE = "file_diff_stage";
        static final String GET_FILES_STAGE = "get_files_stage";
        static final String FINALIZE_REPLICATION_STAGE = "finalize_replication_stage";
    }

}
