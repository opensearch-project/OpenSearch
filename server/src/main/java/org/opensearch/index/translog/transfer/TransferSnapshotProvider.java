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

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
        Function<Long, String> checkpointGenFileNameMapper
    ) throws IOException {
        translogTransferSnapshot = new TranslogCheckpointTransferSnapshot(primaryTerm, generation, readers.size());
        for (TranslogReader reader : readers) {
            final long readerGeneration = reader.getGeneration();
            final long readerPrimaryTerm = reader.getPrimaryTerm();
            final long minTranslogGeneration = reader.getCheckpoint().getMinTranslogGeneration();
            final long checkpointGeneration = reader.getCheckpoint().getGeneration();
            Path translogPath = reader.path();
            Path checkpointPath = location.resolve(checkpointGenFileNameMapper.apply(readerGeneration));
            translogTransferSnapshot.add(
                new TranslogFileSnapshot(readerPrimaryTerm, readerGeneration, translogPath),
                new CheckpointFileSnapshot(readerPrimaryTerm, checkpointGeneration, minTranslogGeneration, checkpointPath)
            );
        }
    }

    public TranslogCheckpointTransferSnapshot get() {
        translogTransferSnapshot.assertInvariants();
        return translogTransferSnapshot;
    }

    static class TranslogCheckpointTransferSnapshot implements TransferSnapshot {

        private final Set<Tuple<TranslogFileSnapshot, CheckpointFileSnapshot>> translogCheckpointFileInfoTupleSet;
        private final int size;
        private final List<Long> generations;
        private CheckpointFileSnapshot latestCheckPointFileSnapshot;
        private TranslogFileSnapshot latestTranslogFileSnapshot;
        private final long generation;
        private long highestGeneration;
        private long lowestGeneration;
        private final long primaryTerm;

        TranslogCheckpointTransferSnapshot(long primaryTerm, long generation, int size) {
            translogCheckpointFileInfoTupleSet = new HashSet<>(size);
            this.size = size;
            this.generations = new LinkedList<>();
            this.generation = generation;
            this.primaryTerm = primaryTerm;
            this.highestGeneration = Long.MIN_VALUE;
            this.lowestGeneration = Long.MAX_VALUE;
        }

        private void add(TranslogFileSnapshot translogFileSnapshot, CheckpointFileSnapshot checkPointFileSnapshot) {
            translogCheckpointFileInfoTupleSet.add(Tuple.tuple(translogFileSnapshot, checkPointFileSnapshot));
            assert translogFileSnapshot.getGeneration() == checkPointFileSnapshot.getGeneration();
            generations.add(translogFileSnapshot.getGeneration());
            if (highestGeneration < translogFileSnapshot.getGeneration()) {
                latestCheckPointFileSnapshot = checkPointFileSnapshot;
                latestTranslogFileSnapshot = translogFileSnapshot;
                highestGeneration = translogFileSnapshot.getGeneration();
            }
            this.lowestGeneration = Math.min(lowestGeneration, translogFileSnapshot.getGeneration());
        }

        private void assertInvariants() {
            assert this.primaryTerm == latestTranslogFileSnapshot.getPrimaryTerm() : "inconsistent primary term";
            assert this.generation == highestGeneration : "inconsistent generation";
            assert translogCheckpointFileInfoTupleSet.size() == size : "inconsistent translog and checkpoint file count";
            assert highestGeneration <= lowestGeneration : "lowest generation is greater than highest generation";
            assert LongStream.iterate(lowestGeneration, i -> i + 1)
                .limit(highestGeneration)
                .boxed()
                .collect(Collectors.toList())
                .equals(generations.stream().sorted().collect(Collectors.toList())) == true : "generation gaps found";
        }

        @Override
        public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
            return translogCheckpointFileInfoTupleSet.stream().map(Tuple::v1).collect(Collectors.toSet());
        }

        @Override
        public TranslogTransferMetadata getTranslogTransferMetadata() {
            return new TranslogTransferMetadata(
                latestTranslogFileSnapshot.getPrimaryTerm(),
                latestTranslogFileSnapshot.getGeneration(),
                latestCheckPointFileSnapshot.getMinTranslogGeneration(),
                translogCheckpointFileInfoTupleSet.size() * 2
            );
        }

        @Override
        public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
            return translogCheckpointFileInfoTupleSet.stream().map(Tuple::v2).collect(Collectors.toSet());
        }

        @Override
        public String toString() {
            return new StringBuilder("TranslogTransferSnapshot [").append(" primary term = ")
                .append(primaryTerm)
                .append(", generation = ")
                .append(generation)
                .append(" ]")
                .toString();
        }
    }
}
