/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationTimer;

import java.io.IOException;

/**
 * ReplicationState implementation to track Segment Replication events.
 *
 * @opensearch.internal
 */
public class SegmentReplicationState implements ReplicationState, ToXContentFragment, Writeable {

    /**
     * The stage of the recovery state
     *
     * @opensearch.internal
     */
    public enum Stage {
        DONE((byte) 0),
        INIT((byte) 1),
        REPLICATING((byte) 2),
        GET_CHECKPOINT_INFO((byte) 3),
        FILE_DIFF((byte) 4),
        GET_FILES((byte) 5),
        FINALIZE_REPLICATION((byte) 6),
        CANCELLED((byte) 7);

        private static final Stage[] STAGES = new Stage[Stage.values().length];

        static {
            for (Stage stage : Stage.values()) {
                assert stage.id() < STAGES.length && stage.id() >= 0;
                STAGES[stage.id] = stage;
            }
        }

        private final byte id;

        Stage(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Stage fromId(byte id) {
            if (id < 0 || id >= STAGES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    private Stage stage;
    private ReplicationLuceneIndex index;

    public ReplicationTimer getOverallTimer() {
        return overallTimer;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    private final ReplicationTimer overallTimer;

    public Replicating getReplicating() {
        return replicating;
    }

    public GetCheckpointInfo getGetCheckpointInfo() {
        return getCheckpointInfo;
    }

    public FileDiff getFileDiff() {
        return fileDiff;
    }

    public GetFile getGetFile() {
        return getFile;
    }

    public FinalizeReplication getFinalizeReplication() {
        return finalizeReplication;
    }

    private final Replicating replicating;

    private final GetCheckpointInfo getCheckpointInfo;

    private final FileDiff fileDiff;

    private final GetFile getFile;

    private final FinalizeReplication finalizeReplication;
    private long replicationId;

    private final ShardRouting shardRouting;

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    private DiscoveryNode sourceNode;

    private DiscoveryNode targetNode;

    public long getTotalNumberOfSegRepEvents() {
        return totalNumberOfSegRepEvents;
    }

    private long totalNumberOfSegRepEvents;

    public SegmentReplicationState(
        ShardRouting shardRouting,
        ReplicationLuceneIndex index,
        long replicationId,
        DiscoveryNode sourceNode,
        DiscoveryNode targetNode
    ) {
        this.index = index;
        this.shardRouting = shardRouting;
        this.replicationId = replicationId;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        // Timing data will have as many entries as stages, plus one
        overallTimer = new ReplicationTimer();
        replicating = new Replicating();
        getCheckpointInfo = new GetCheckpointInfo();
        fileDiff = new FileDiff();
        getFile = new GetFile();
        finalizeReplication = new FinalizeReplication();
        setStage(Stage.INIT);
        totalNumberOfSegRepEvents++;
    }

    public SegmentReplicationState(StreamInput in) throws IOException {
        index = new ReplicationLuceneIndex(in);
        shardRouting = new ShardRouting(in);
        stage = in.readEnum(Stage.class);
        replicationId = in.readLong();
        overallTimer = new ReplicationTimer(in);
        replicating = new Replicating(in);
        getCheckpointInfo = new GetCheckpointInfo(in);
        fileDiff = new FileDiff(in);
        getFile = new GetFile(in);
        finalizeReplication = new FinalizeReplication(in);
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
        totalNumberOfSegRepEvents = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        shardRouting.writeTo(out);
        out.writeEnum(stage);
        out.writeLong(replicationId);
        overallTimer.writeTo(out);
        // stageTimer.writeTo(out);
        replicating.writeTo(out);
        getCheckpointInfo.writeTo(out);
        fileDiff.writeTo(out);
        getFile.writeTo(out);
        finalizeReplication.writeTo(out);
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        out.writeLong(totalNumberOfSegRepEvents);

    }

    @Override
    public ReplicationLuceneIndex getIndex() {
        return index;
    }

    public long getReplicationId() {
        return replicationId;
    }

    @Override
    public ReplicationTimer getTimer() {
        return overallTimer;
    }

    public Stage getStage() {
        return this.stage;
    }

    protected void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            assert false : "can't move replication to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])";
            throw new IllegalStateException(
                "can't move replication to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])"
            );
        }
        stage = next;
    }

    public void setStage(Stage stage) {
        switch (stage) {
            case INIT:
                this.stage = Stage.INIT;
                overallTimer.reset();
                getReplicating().reset();
                getGetCheckpointInfo().reset();
                getFileDiff().reset();
                getGetFile().reset();
                getFinalizeReplication().reset();
                break;
            case REPLICATING:
                validateAndSetStage(Stage.INIT, stage);
                // only start the overall timer once we've started replication
                overallTimer.start();
                getReplicating().start();
                break;
            case GET_CHECKPOINT_INFO:
                validateAndSetStage(Stage.REPLICATING, stage);
                getReplicating().stop();
                getGetCheckpointInfo().start();
                break;
            case FILE_DIFF:
                validateAndSetStage(Stage.GET_CHECKPOINT_INFO, stage);
                getGetCheckpointInfo().stop();
                getFileDiff().start();
                break;
            case GET_FILES:
                validateAndSetStage(Stage.FILE_DIFF, stage);
                getFileDiff().stop();
                getGetFile().start();
                break;
            case FINALIZE_REPLICATION:
                validateAndSetStage(Stage.GET_FILES, stage);
                getGetFile().stop();
                getFinalizeReplication().start();
                break;
            case DONE:
                validateAndSetStage(Stage.FINALIZE_REPLICATION, stage);
                getFinalizeReplication().stop();
                // add the overall timing data
                overallTimer.stop();
                break;
            case CANCELLED:
                if (this.stage == Stage.DONE) {
                    throw new IllegalStateException("can't move replication to Cancelled state from Done.");
                }
                this.stage = Stage.CANCELLED;
                overallTimer.stop();
                break;
            default:
                throw new IllegalArgumentException("unknown SegmentReplicationState.Stage [" + stage + "]");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {

        builder.field(Fields.ID, shardRouting.shardId().id());
        builder.field(Fields.STAGE, getStage());
        builder.field(Fields.REPLICATION_ID, getReplicationId());
        builder.field(Fields.NUMBER_OF_SEGMENT_REPLICATION_EVENTS, getTotalNumberOfSegRepEvents());
        builder.timeField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, getTimer().startTime());
        if (getTimer().stopTime() > 0) {
            builder.timeField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, getTimer().stopTime());
        }
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(getTimer().time()));
        if (sourceNode != null) {
            builder.startObject(SegmentReplicationState.Fields.SOURCE);
            builder.field(Fields.ID, sourceNode.getId());
            builder.field(Fields.HOST, sourceNode.getHostName());
            builder.field(Fields.TRANSPORT_ADDRESS, sourceNode.getAddress().toString());
            builder.field(SegmentReplicationState.Fields.IP, sourceNode.getHostAddress());
            builder.field(SegmentReplicationState.Fields.NAME, sourceNode.getName());
            builder.endObject();
        }

        if (targetNode != null) {
            builder.startObject(Fields.TARGET);
            builder.field(Fields.ID, targetNode.getId());
            builder.field(Fields.HOST, targetNode.getHostName());
            builder.field(Fields.TRANSPORT_ADDRESS, targetNode.getAddress().toString());
            builder.field(Fields.IP, targetNode.getHostAddress());
            builder.field(Fields.NAME, targetNode.getName());
            builder.endObject();
        }
        builder.startObject(Fields.REPLICATING_STAGE);
        replicating.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.GET_CHECKPOINT_INFO_STAGE);
        getCheckpointInfo.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.FILE_DIFF_STAGE);
        fileDiff.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.GET_FILES_STAGE);
        index.toXContent(builder, params);
        getFile.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.FINALIZE_REPLICATION_STAGE);
        finalizeReplication.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    static final class Fields {
        static final String ID = "id";
        static final String REPLICATION_ID = "replication_id";
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

        static final String SIZE_OF_FILES_IN_BYTES = "size_of_files_in_bytes";

        static final String NUMBER_OF_SEGMENT_REPLICATION_EVENTS = "number_of_segment_replication_events";

        static final String INDEX = "index";
        static final String TOTAL_GET_FILES_STAGE_TIME_IN_MILLIS = "total_get_files_stage_in_millis";
        static final String REPLICATING_STAGE = "replicating_stage";
        static final String GET_CHECKPOINT_INFO_STAGE = "get_checkpoint_info_stage";
        static final String FILE_DIFF_STAGE = "file_diff_stage";
        static final String GET_FILES_STAGE = "get_files_stage";
        static final String FINALIZE_REPLICATION_STAGE = "finalize_replication_stage";
    }

    /**
     * Starts Replicating
     *
     * @opensearch.internal
     */
    public static class Replicating extends ReplicationTimer implements ToXContentFragment, Writeable {

        public Replicating() {}

        public Replicating(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        public void reset() {
            super.reset();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

    /**
     * Gets checkpoint info from source
     *
     * @opensearch.internal
     */
    public static class GetCheckpointInfo extends ReplicationTimer implements ToXContentFragment, Writeable {

        public GetCheckpointInfo() {}

        public GetCheckpointInfo(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        public void reset() {
            super.reset();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

    /**
     * Computes File Diff
     *
     * @opensearch.internal
     */
    public static class FileDiff extends ReplicationTimer implements ToXContentFragment, Writeable {

        private volatile long sizeOfFiles;

        public FileDiff() {}

        public FileDiff(StreamInput in) throws IOException {
            super(in);
            sizeOfFiles = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(sizeOfFiles);
        }

        public void reset() {
            super.reset();
            sizeOfFiles = 0;
        }

        public long getSizeOfFiles() {
            return sizeOfFiles;
        }

        public void setSizeOfFiles(long sizeOfFiles) {
            this.sizeOfFiles = sizeOfFiles;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.SIZE_OF_FILES_IN_BYTES, getSizeOfFiles());
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

    /**
     * Gets necessary files from source
     *
     * @opensearch.internal
     */
    public static class GetFile extends ReplicationTimer implements ToXContentFragment, Writeable {

        public GetFile() {}

        public GetFile(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        public void reset() {
            super.reset();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.TOTAL_GET_FILES_STAGE_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

    /**
     * Finalizes Replication
     *
     * @opensearch.internal
     */
    public static class FinalizeReplication extends ReplicationTimer implements ToXContentFragment, Writeable {

        public FinalizeReplication() {}

        public FinalizeReplication(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        public void reset() {
            super.reset();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }
}
