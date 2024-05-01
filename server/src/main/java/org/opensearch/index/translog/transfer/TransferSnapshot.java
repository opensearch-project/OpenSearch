/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import java.util.Set;

/**
 * The snapshot of the generational translog and checkpoint files and it's corresponding metadata that is transferred
 * to the {@link TransferService}
 *
 * @opensearch.internal
 */
public interface TransferSnapshot {

    /**
     * The translog transfer metadata of this {@link TransferSnapshot}
     * @return the translog transfer metadata
     */
    TranslogTransferMetadata getTranslogTransferMetadata();

    /**
     * The set of generational translog and checkpoint snapshots
     *  @return the set of {@link TranslogCheckpointSnapshot}
     */
    Set<TranslogCheckpointSnapshot> getTranslogCheckpointSnapshots();
}
