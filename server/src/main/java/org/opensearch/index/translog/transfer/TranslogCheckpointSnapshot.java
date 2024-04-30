/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.Checkpoint;
import org.opensearch.index.translog.TranslogCheckedContainer;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.index.translog.transfer.TranslogTransferManager.CHECKPOINT_FILE_DATA_KEY;

/**
 * Snapshot of a single translog generational files that gets transferred
 *
 * @opensearch.internal
 */
public class TranslogCheckpointSnapshot {

    private final long primaryTerm;
    private final long generation;
    private final long minTranslogGeneration;
    private final Path translogPath;
    private final Path checkpointPath;
    private final Long translogChecksum;
    private final Long checkpointChecksum;
    private final Checkpoint checkpoint;
    private final long checkpointGeneration;

    public TranslogCheckpointSnapshot(
        long primaryTerm,
        long generation,
        long minTranslogGeneration,
        Path translogPath,
        Path checkpointPath,
        Long translogChecksum,
        Long checkpointChecksum,
        Checkpoint checkpoint,
        long checkpointGeneration
    ) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.translogPath = translogPath;
        this.checkpointPath = checkpointPath;
        this.translogChecksum = translogChecksum;
        this.checkpointChecksum = checkpointChecksum;
        this.checkpoint = checkpoint;
        this.checkpointGeneration = checkpointGeneration;
    }

    public String getTranslogFileName() {
        return translogPath.getFileName().toString();
    }

    public String getCheckpointFileName() {
        return checkpointPath.getFileName().toString();
    }

    public long getTranslogFileContentLength() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(translogPath, StandardOpenOption.READ)) {
            return fileChannel.size();
        }
    }

    public long getCheckpointFileContentLength() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(checkpointPath, StandardOpenOption.READ)) {
            return fileChannel.size();
        }
    }

    public long getGeneration() {
        return generation;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getCheckpointGeneration() {
        return checkpointGeneration;
    }

    TransferFileSnapshot getTranslogFileSnapshot() throws IOException {
        return new TranslogFileSnapshot(primaryTerm, generation, translogPath, translogChecksum);
    }

    TransferFileSnapshot getCheckpointFileSnapshot() throws IOException {
        return new CheckpointFileSnapshot(primaryTerm, generation, minTranslogGeneration, checkpointPath, checkpointChecksum);
    }

    TransferFileSnapshot getTranslogFileSnapshotWithMetadata() throws IOException {
        Map<String, String> metadata = createCheckpointDataAsObjectMetadata();
        TransferFileSnapshot translogFileSnapshot = getTranslogFileSnapshot();
        translogFileSnapshot.setMetadata(metadata);
        return translogFileSnapshot;
    }

    private Map<String, String> createCheckpointDataAsObjectMetadata() throws IOException {
        Map<String, String> metadata = new HashMap<>();

        byte[] fileBytes = Checkpoint.createCheckpointBytes(checkpointPath, checkpoint);

        // Do checksum validation here.
        TranslogCheckedContainer translogCheckedContainer = new TranslogCheckedContainer(fileBytes);
        Long calculatedChecksum = translogCheckedContainer.getChecksum();

        if (checkpointChecksum != null && !checkpointChecksum.equals(calculatedChecksum)) {
            throw new TranslogUploadFailedException("Checksum validation of checkpoint file failed for translog file:");
        }

        // Set the file data value
        String fileDataBase64String = Base64.getEncoder().encodeToString(fileBytes);
        metadata.put(CHECKPOINT_FILE_DATA_KEY, fileDataBase64String);

        return metadata;
    }

}
