/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

import java.io.IOException;
import java.util.Set;

/**
 * The snapshot of the generational translog and checkpoint files and it's corresponding metadata that is transferred
 * to the {@link TransferService}
 *
 * @opensearch.internal
 */
public interface TransferSnapshot {

    /**
     * The snapshot of the checkpoint generational files
     * @return the set of {@link CheckpointFileSnapshot}
     */
    Set<TransferFileSnapshot> getCheckpointFileSnapshots();

    /**
     * The snapshot of the translog generational files
     * @return the set of {@link TranslogFileSnapshot}
     */
    Set<TransferFileSnapshot> getTranslogFileSnapshots();

    /**
     * The translog transfer metadata of this {@link TransferSnapshot}
     * @return the translog transfer metadata
     */
    TranslogTransferMetadata getTranslogTransferMetadata();

    /**
     * The snapshot of the translog generational files having checkpoint file inputStream as metadata
     * @return the set of translog files having checkpoint file inputStream as metadata.
     */
    Set<TransferFileSnapshot> getTranslogFileSnapshotWithMetadata() throws IOException;
}
