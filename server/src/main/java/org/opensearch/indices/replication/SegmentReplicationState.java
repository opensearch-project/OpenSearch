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

        INIT((byte) 1);

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

    public SegmentReplicationState() {
        this.stage = Stage.INIT;
    }

    private Stage stage;

    @Override
    public ReplicationLuceneIndex getIndex() {
        // TODO
        return null;
    }

    @Override
    public ReplicationTimer getTimer() {
        // TODO
        return null;
    }

    public Stage getStage() {
        return stage;
    }

    public void setStage(Stage stage) {
        this.stage = stage;
    }
}
