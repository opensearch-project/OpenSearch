/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.translog.TranslogReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;

/**
 * Provider for a {@link TransferSnapshot} which builds the snapshot from the translog and checkpoint files present on the local-disk
 *
 * @opensearch.internal
 */
public class TransferSnapshotProvider implements Supplier<TransferSnapshot> {

    private final TranslogCheckpointTransferSnapshot translogTransferSnapshot;

    public TransferSnapshotProvider(
        long primaryTerm,
        long generation,
        Path location,
        List<TranslogReader> readers,
        Function<Long, String> checkpointFileNameMapper
    ) throws IOException {
        translogTransferSnapshot = new TranslogCheckpointTransferSnapshot(primaryTerm, generation, readers.size());
        for (TranslogReader reader : readers) {
            final long readerGeneration = reader.getGeneration();
            final long readerPrimaryTerm = reader.getPrimaryTerm();
            final long minTranslogGeneration = reader.getCheckpoint().getMinTranslogGeneration();
            Path translogPath = reader.path();
            Path checkpointPath = location.resolve(checkpointFileNameMapper.apply(readerGeneration));
            translogTransferSnapshot.add(
                buildTranslogFileInfo(translogPath.toFile(), readerPrimaryTerm, readerGeneration),
                buildCheckpointFileInfo(checkpointPath.toFile(), readerPrimaryTerm, minTranslogGeneration)
            );
        }
    }

    public TranslogCheckpointTransferSnapshot get() {
        translogTransferSnapshot.assertInvariants();
        return translogTransferSnapshot;
    }

    private TranslogFileSnapshot buildTranslogFileInfo(File file, long primaryTerm, long generation) throws IOException {
        TranslogFileSnapshot fileSnapshot;
        try (CheckedInputStream stream = new CheckedInputStream(new FileInputStream(file), new CRC32())) {
            byte[] content = stream.readAllBytes();
            long checksum = stream.getChecksum().getValue();
            fileSnapshot = new TranslogFileSnapshot(primaryTerm, generation, file.getName(), file.toPath(), checksum, content);
        }
        return fileSnapshot;
    }

    private CheckpointFileSnapshot buildCheckpointFileInfo(File file, long primaryTerm, long minTranslogGeneration) throws IOException {
        CheckpointFileSnapshot fileSnapshot;
        try (CheckedInputStream stream = new CheckedInputStream(new FileInputStream(file), new CRC32())) {
            byte[] content = stream.readAllBytes();
            long checksum = stream.getChecksum().getValue();
            fileSnapshot = new CheckpointFileSnapshot(primaryTerm, minTranslogGeneration, file.getName(), file.toPath(), checksum, content);
        }
        return fileSnapshot;
    }

    static class TranslogCheckpointTransferSnapshot implements TransferSnapshot {

        private final Set<Tuple<TranslogFileSnapshot, CheckpointFileSnapshot>> translogCheckpointFileInfoTupleSet;
        private final int size;
        private CheckpointFileSnapshot latestCheckPointFileSnapshot;
        private TranslogFileSnapshot latestTranslogFileSnapshot;
        private long generation;
        private long highestGeneration;
        private long primaryTerm;

        TranslogCheckpointTransferSnapshot(long primaryTerm, long generation, int size) {
            translogCheckpointFileInfoTupleSet = new HashSet<>(size);
            this.size = size;
            this.generation = generation;
            this.primaryTerm = primaryTerm;
        }

        private void add(TranslogFileSnapshot translogFileSnapshot, CheckpointFileSnapshot checkPointFileSnapshot) {
            translogCheckpointFileInfoTupleSet.add(Tuple.tuple(translogFileSnapshot, checkPointFileSnapshot));
            if (highestGeneration < translogFileSnapshot.getGeneration()) {
                latestCheckPointFileSnapshot = checkPointFileSnapshot;
                latestTranslogFileSnapshot = translogFileSnapshot;
                highestGeneration = translogFileSnapshot.getGeneration();
            }
        }

        private void assertInvariants() {
            assert this.primaryTerm == latestTranslogFileSnapshot.getPrimaryTerm() : "inconsistent primary term";
            assert this.generation == highestGeneration : "inconsistent generation";
            assert translogCheckpointFileInfoTupleSet.size() == size : "inconsistent translog and checkpoint file count";
        }

        public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
            return translogCheckpointFileInfoTupleSet.stream().map(tuple -> tuple.v1()).collect(Collectors.toSet());
        }

        @Override
        public TranslogTransferMetadata getTranslogTransferMetadata() {
            return new TranslogTransferMetadata(
                latestTranslogFileSnapshot.getPrimaryTerm(),
                latestTranslogFileSnapshot.getGeneration(),
                latestCheckPointFileSnapshot.getMinTranslogGeneration(),
                translogCheckpointFileInfoTupleSet.size()
            );
        }

        public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
            return translogCheckpointFileInfoTupleSet.stream().map(tuple -> tuple.v2()).collect(Collectors.toSet());
        }
    }
}
