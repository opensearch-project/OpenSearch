/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.util.Set;

/**
 * The snapshot of the files and it's metadata that is transferred to the {@link TransferService}
 *
 * @opensearch.internal
 */
public interface TransferSnapshot {

    Set<TransferFileSnapshot> getCheckpointFileSnapshots();

    Set<TransferFileSnapshot> getTranslogFileSnapshots();

    TranslogTransferMetadata getTranslogTransferMetadata();
}
