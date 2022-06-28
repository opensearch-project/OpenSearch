/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationTimer;

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

        REPLICATING((byte) 2);

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
    private final ReplicationTimer timer;

    public SegmentReplicationState(ReplicationLuceneIndex index) {
        stage = Stage.INIT;
        this.index = index;
        timer = new ReplicationTimer();
        timer.start();
    }

    @Override
    public ReplicationLuceneIndex getIndex() {
        return index;
    }

    @Override
    public ReplicationTimer getTimer() {
        return timer;
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
        stage = next;
    }

    public void setStage(Stage stage) {
        switch (stage) {
            case INIT:
                this.stage = Stage.INIT;
                getIndex().reset();
                break;
            case REPLICATING:
                validateAndSetStage(Stage.INIT, stage);
                getIndex().start();
                break;
            case DONE:
                validateAndSetStage(Stage.REPLICATING, stage);
                getIndex().stop();
                getTimer().stop();
                break;
            default:
                throw new IllegalArgumentException("unknown SegmentReplicationState.Stage [" + stage + "]");
        }
    }
}
