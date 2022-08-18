/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.common.collect.Tuple;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationTimer;

import java.util.ArrayList;
import java.util.List;

/**
 * ReplicationState implementation to track Segment Replication events.
 *
 * @opensearch.internal
 */
public class SegmentReplicationState implements ReplicationState {

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
    private final ReplicationLuceneIndex index;
    private final ReplicationTimer overallTimer;
    private final ReplicationTimer stageTimer;
    private final List<Tuple<String, Long>> timingData;
    private long replicationId;

    public SegmentReplicationState(ReplicationLuceneIndex index) {
        stage = Stage.INIT;
        this.index = index;
        // Timing data will have as many entries as stages, plus one
        // additional entry for the overall timer
        timingData = new ArrayList<>(Stage.values().length + 1);
        overallTimer = new ReplicationTimer();
        stageTimer = new ReplicationTimer();
        stageTimer.start();
        // set an invalid value by default
        this.replicationId = -1L;
    }

    public SegmentReplicationState(ReplicationLuceneIndex index, long replicationId) {
        this(index);
        this.replicationId = replicationId;
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

    public List<Tuple<String, Long>> getTimingData() {
        return timingData;
    }

    public Stage getStage() {
        return stage;
    }

    protected void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            assert false : "can't move replication to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])";
            throw new IllegalStateException(
                "can't move replication to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])"
            );
        }
        // save the timing data for the current step
        stageTimer.stop();
        timingData.add(new Tuple<>(stage.name(), stageTimer.time()));
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
                timingData.add(new Tuple<>("OVERALL", overallTimer.time()));
                break;
            default:
                throw new IllegalArgumentException("unknown SegmentReplicationState.Stage [" + stage + "]");
        }
    }
}
