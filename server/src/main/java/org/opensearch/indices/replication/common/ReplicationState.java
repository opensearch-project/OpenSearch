/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.indices.recovery.RecoveryIndex;
import org.opensearch.indices.recovery.Timer;

public class ReplicationState {

    protected Timer timer;
    protected RecoveryIndex index;

    protected ReplicationState() {
        // Empty default constructor for subclasses
    }

    protected ReplicationState(RecoveryIndex index) {
        this.index = index;
        timer = new Timer();
        timer.start();
    }

    public Timer getTimer() {
        return timer;
    }

    public RecoveryIndex getIndex() {
        return index;
    }
}
