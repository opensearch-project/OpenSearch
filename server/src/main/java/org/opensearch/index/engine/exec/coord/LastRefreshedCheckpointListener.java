/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.seqno.LocalCheckpointTracker;

import java.util.concurrent.atomic.AtomicLong;

final class LastRefreshedCheckpointListener implements ReferenceManager.RefreshListener {
    final AtomicLong refreshedCheckpoint;
    volatile AtomicLong pendingCheckpoint;
    private final LocalCheckpointTracker localCheckpointTracker;

    LastRefreshedCheckpointListener(LocalCheckpointTracker localCheckpointTracker) {
        final long processedCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
        this.refreshedCheckpoint = new AtomicLong(processedCheckpoint);
        this.pendingCheckpoint = new AtomicLong(processedCheckpoint);
        this.localCheckpointTracker = localCheckpointTracker;
    }

    @Override
    public void beforeRefresh() {
        // all changes until this point should be visible after refresh
        pendingCheckpoint.updateAndGet(curr -> Math.max(curr, localCheckpointTracker.getProcessedCheckpoint()));
    }

    @Override
    public void afterRefresh(boolean didRefresh) {
        if (didRefresh) {
            updateRefreshedCheckpoint(pendingCheckpoint.get());
        }
    }

    void updateRefreshedCheckpoint(long checkpoint) {
        refreshedCheckpoint.updateAndGet(curr -> Math.max(curr, checkpoint));
        assert refreshedCheckpoint.get() >= checkpoint : refreshedCheckpoint.get() + " < " + checkpoint;
        // This shouldn't be required ideally, but we're also invoking this method from refresh as of now.
        // This change is added as safety check to ensure that our checkpoint values are consistent at all times.
        pendingCheckpoint.updateAndGet(curr -> Math.max(curr, checkpoint));
    }
}
