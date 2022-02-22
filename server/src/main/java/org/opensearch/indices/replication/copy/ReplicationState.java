/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.Timer;

public class ReplicationState {

    private Timer timer;
    private RecoveryState.Index index;
    private Stage stage;

    public ReplicationState(RecoveryState.Index index) {
        this.index = index;
        this.timer = new Timer();
        stage = Stage.INACTIVE;
        timer.start();
    }

    public ReplicationState() {
        stage = Stage.INACTIVE;
    }

    public Timer getTimer() {
        return timer;
    }

    public RecoveryState.Index getIndex() {
        return index;
    }

    public enum Stage {
        // TODO: Add more steps here.
        INACTIVE((byte) 0),

        ACTIVE((byte) 1);

        private static final ReplicationState.Stage[] STAGES = new ReplicationState.Stage[ReplicationState.Stage.values().length];

        static {
            for (ReplicationState.Stage stage : ReplicationState.Stage.values()) {
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

        public static ReplicationState.Stage fromId(byte id) {
            if (id < 0 || id >= STAGES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    public synchronized Stage getStage() {
        return this.stage;
    }
    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized void setStage(Stage stage) {
        this.stage = stage;
    }
}
