/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.opensearch.cluster.routing.allocation.AllocationService;

public abstract class AbstractTieringStateProcessor implements TieringStateProcessor {

    protected final IndexTieringState nextState;
    protected final AbstractTieringStateProcessor nextStateProcessor;
    protected final AllocationService allocationService;

    public AbstractTieringStateProcessor(IndexTieringState nextState, AllocationService allocationService) {
        this.allocationService = allocationService;
        this.nextState = nextState;
        if (nextState == null) {
            this.nextStateProcessor = null;
            return;
        }

        switch (nextState) {
            case PENDING_START:
                this.nextStateProcessor = new PendingStartTieringStateProcessor(allocationService);
                break;
            case IN_PROGRESS:
                this.nextStateProcessor = new RunningTieringStateProcessor(allocationService);
                break;
            case PENDING_COMPLETION:
                this.nextStateProcessor = new PendingCompleteTieringStateProcessor(allocationService);
                break;
            case COMPLETED:
                this.nextStateProcessor = new CompletedTieringStateProcessor(allocationService);
                break;
            default:
                this.nextStateProcessor = null;
        }
    }
}
