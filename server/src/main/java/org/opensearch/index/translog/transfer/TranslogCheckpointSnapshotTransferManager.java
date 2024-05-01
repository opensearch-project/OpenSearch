/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;

import java.util.Map;
import java.util.Set;

/**
 * Translog transfer manager to transfer {@link TranslogCheckpointSnapshot}
 *
 * @opensearch.internal
 */
public interface TranslogCheckpointSnapshotTransferManager {

    public void transferTranslogCheckpointSnapshot(
        Set<TranslogCheckpointSnapshot> toUpload,
        Map<Long, BlobPath> blobPathMap,
        LatchedActionListener<TranslogCheckpointSnapshot> latchedActionListener,
        WritePriority writePriority
    ) throws Exception;
}
