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
import org.opensearch.indices.replication.common.ReplicationState;

public class SegmentReplicationState extends ReplicationState {

    private Stage stage;

    public SegmentReplicationState(RecoveryIndex index) {
        super(index);
        stage = Stage.INACTIVE;
    }

    public SegmentReplicationState() {
        stage = Stage.INACTIVE;
    }

    public synchronized Stage getStage() {
        return this.stage;
    }

    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized void setStage(Stage stage) {
        this.stage = stage;
    }

    /**
     * THis class duplicates the purpose/functionality of {@link RecoveryState.Stage}
     * so this temporary implementation simply aliases the enums from the other class.
     * TODO Merge this class with the above Stage class once segrep lifecycle is finalized
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
}
