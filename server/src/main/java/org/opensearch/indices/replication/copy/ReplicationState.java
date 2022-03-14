/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.opensearch.indices.recovery.RecoveryIndex;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.Timer;
import org.opensearch.indices.replication.common.RState;

public class ReplicationState implements RState {

    private Timer timer;
    private RecoveryIndex index;
    private Stage stage;

    public ReplicationState(RecoveryIndex index) {
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

    public RecoveryIndex getIndex() {
        return index;
    }

    /**
     * THis class duplicates the purpose/functionality of {@link RecoveryState.Stage}
     * so this temporary implementation simply aliases the enums from the other class.
     */
    public enum Stage {
        // TODO: Add more steps here.
        INACTIVE(RecoveryState.Stage.INIT),

        ACTIVE(RecoveryState.Stage.INDEX);

        private final byte id;

        Stage(RecoveryState.Stage recoveryStage) {
            this.id = recoveryStage.id();
        }

        public byte id() {
            return id;
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
