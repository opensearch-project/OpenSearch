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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationTimer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ReplicationState implementation to track Segment Replication events.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.2.0")
public class SegmentReplicationState implements ReplicationState, ToXContentFragment, Writeable {

    /**
     * The stage of the recovery state
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.2.0")
    public enum Stage {
        DONE((byte) 0),
        INIT((byte) 1),
        REPLICATING((byte) 2),
        GET_CHECKPOINT_INFO((byte) 3),
        FILE_DIFF((byte) 4),
        GET_FILES((byte) 5),
        FINALIZE_REPLICATION((byte) 6);

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

    private final ReplicationTimer overallTimer;
    private final Map<String, Long> timingData;
    private final ReplicationTimer stageTimer;
    private long replicationId;
    private final ShardRouting shardRouting;
    private String sourceDescription;
    private DiscoveryNode targetNode;

    private ReplicationCheckpoint latestReplicationCheckpoint;

    public ShardRouting getShardRouting() {
        return shardRouting;
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

    public String getSourceDescription() {

        return sourceDescription;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public Map<String, Long> getTimingData() {
        return timingData;
    }

    public TimeValue getReplicatingStageTime() {
        long time = timingData.getOrDefault(Stage.REPLICATING.toString(), 0L);
        return new TimeValue(time);
    }

    public TimeValue getGetCheckpointInfoStageTime() {
        long time = timingData.getOrDefault(Stage.GET_CHECKPOINT_INFO.toString(), 0L);
        return new TimeValue(time);
    }

    public TimeValue getFileDiffStageTime() {
        long time = timingData.getOrDefault(Stage.FILE_DIFF.toString(), 0L);
        return new TimeValue(time);
    }

    public TimeValue getGetFileStageTime() {
        long time = timingData.getOrDefault(Stage.GET_FILES.toString(), 0L);
        return new TimeValue(time);
    }

    public TimeValue getFinalizeReplicationStageTime() {
        long time = timingData.getOrDefault(Stage.FINALIZE_REPLICATION.toString(), 0L);
        return new TimeValue(time);
    }

    public ReplicationCheckpoint getLatestReplicationCheckpoint() {
        return this.latestReplicationCheckpoint;
    }

    public SegmentReplicationState(
        ShardRouting shardRouting,
        ReplicationLuceneIndex index,
        long replicationId,
        String sourceDescription,
        DiscoveryNode targetNode
    ) {
        this.index = index;
        this.shardRouting = shardRouting;
        this.replicationId = replicationId;
        this.sourceDescription = sourceDescription;
        this.targetNode = targetNode;
        // Timing data will have as many entries as stages, plus one
        timingData = new ConcurrentHashMap<>(Stage.values().length + 1);
        overallTimer = new ReplicationTimer();
        stageTimer = new ReplicationTimer();
        setStage(Stage.INIT);
        stageTimer.start();
    }

    public SegmentReplicationState(StreamInput in) throws IOException {
        index = new ReplicationLuceneIndex(in);
        shardRouting = new ShardRouting(in);
        stage = in.readEnum(Stage.class);
        replicationId = in.readLong();
        overallTimer = new ReplicationTimer(in);
        stageTimer = new ReplicationTimer(in);
        timingData = in.readMap(StreamInput::readString, StreamInput::readLong);
        sourceDescription = in.readString();
        targetNode = new DiscoveryNode(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        shardRouting.writeTo(out);
        out.writeEnum(stage);
        out.writeLong(replicationId);
        overallTimer.writeTo(out);
        stageTimer.writeTo(out);

        // Copy of timingData is created to avoid concurrent modification of timingData map.
        Map<String, Long> timingDataCopy = new HashMap<>();
        for (Map.Entry<String, Long> entry : timingData.entrySet()) {
            timingDataCopy.put(entry.getKey(), entry.getValue());
        }
        out.writeMap(timingDataCopy, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeString(sourceDescription);
        targetNode.writeTo(out);
    }

    protected void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            assert false : "can't move replication to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])";
            throw new IllegalStateException(
                "can't move replication to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])"
            );
        }
        stopTimersAndSetStage(next);
    }

    private void stopTimersAndSetStage(Stage next) {
        // save the timing data for the current step
        stageTimer.stop();
        timingData.put(stage.name(), stageTimer.time());
        // restart the step timer
        stageTimer.reset();
        stageTimer.start();
        stage = next;
    }

    public void setStage(Stage stage) {
        switch (stage) {
            case INIT:
                this.stage = Stage.INIT;
                break;
            case REPLICATING:
                validateAndSetStage(Stage.INIT, stage);
                // only start the overall timer once we've started replication
                overallTimer.start();
                break;
            case GET_CHECKPOINT_INFO:
                validateAndSetStage(Stage.REPLICATING, stage);
                break;
            case FILE_DIFF:
                validateAndSetStage(Stage.GET_CHECKPOINT_INFO, stage);
                break;
            case GET_FILES:
                validateAndSetStage(Stage.FILE_DIFF, stage);
                break;
            case FINALIZE_REPLICATION:
                validateAndSetStage(Stage.GET_FILES, stage);
                break;
            case DONE:
                validateAndSetStage(Stage.FINALIZE_REPLICATION, stage);
                // add the overall timing data
                overallTimer.stop();
                timingData.put("OVERALL", overallTimer.time());
                break;
            default:
                throw new IllegalArgumentException("unknown SegmentReplicationState.Stage [" + stage + "]");
        }
    }

    public void setLatestReplicationCheckpoint(ReplicationCheckpoint latestReplicationCheckpoint) {
        this.latestReplicationCheckpoint = latestReplicationCheckpoint;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {

        builder.field(Fields.INDEX_NAME, shardRouting.index().getName());
        builder.field(Fields.ID, shardRouting.shardId().id());
        builder.field(Fields.STAGE, getStage());
        builder.timeField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, getTimer().startTime());
        if (getTimer().stopTime() > 0) {
            builder.timeField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, getTimer().stopTime());
        }
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(getTimer().time()));
        builder.field(Fields.SOURCE, getSourceDescription());

        builder.startObject(Fields.TARGET);
        builder.field(Fields.ID, targetNode.getId());
        builder.field(Fields.HOST, targetNode.getHostName());
        builder.field(Fields.TRANSPORT_ADDRESS, targetNode.getAddress().toString());
        builder.field(Fields.IP, targetNode.getHostAddress());
        builder.field(Fields.NAME, targetNode.getName());
        builder.endObject();

        builder.startObject(SegmentReplicationState.Fields.INDEX);
        index.toXContent(builder, params);
        builder.endObject();

        builder.field(Fields.REPLICATING_STAGE, getReplicatingStageTime());
        builder.field(Fields.GET_CHECKPOINT_INFO_STAGE, getGetCheckpointInfoStageTime());
        builder.field(Fields.FILE_DIFF_STAGE, getFileDiffStageTime());
        builder.field(Fields.GET_FILES_STAGE, getGetFileStageTime());
        builder.field(Fields.FINALIZE_REPLICATION_STAGE, getFinalizeReplicationStageTime());

        return builder;
    }

    static final class Fields {
        static final String ID = "id";
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

        static final String INDEX = "index";

        static final String INDEX_NAME = "index_name";
        static final String REPLICATING_STAGE = "replicating_stage";
        static final String GET_CHECKPOINT_INFO_STAGE = "get_checkpoint_info_stage";
        static final String FILE_DIFF_STAGE = "file_diff_stage";
        static final String GET_FILES_STAGE = "get_files_stage";
        static final String FINALIZE_REPLICATION_STAGE = "finalize_replication_stage";
    }
}
