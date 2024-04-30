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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

/**
 * Implementation for a {@link TransferSnapshot} which builds the snapshot from the translog and checkpoint files present on the local-disk
 *
 * @opensearch.internal
 */
public class TranslogCheckpointTransferSnapshot implements TransferSnapshot, Closeable {

    private final Set<Tuple<TranslogFileSnapshot, CheckpointFileSnapshot>> translogCheckpointFileInfoTupleSet;
    private final Set<TranslogCheckpointSnapshot> translogCheckpointSnapshotSet;
    private final int size;
    private final long generation;
    private final long primaryTerm;
    private long minTranslogGeneration;

    private String nodeId;

    TranslogCheckpointTransferSnapshot(long primaryTerm, long generation, int size, String nodeId) {
        translogCheckpointFileInfoTupleSet = new HashSet<>(size);
        translogCheckpointSnapshotSet = new HashSet<>(size);
        this.size = size;
        this.generation = generation;
        this.primaryTerm = primaryTerm;
        this.nodeId = nodeId;
    }

    private void add(
        TranslogFileSnapshot translogFileSnapshot,
        CheckpointFileSnapshot checkPointFileSnapshot,
        TranslogCheckpointSnapshot translogCheckpointSnapshot
    ) {
        translogCheckpointFileInfoTupleSet.add(Tuple.tuple(translogFileSnapshot, checkPointFileSnapshot));

        translogCheckpointSnapshotSet.add(translogCheckpointSnapshot);
        assert translogFileSnapshot.getGeneration() == checkPointFileSnapshot.getGeneration();
    }

    private void setMinTranslogGeneration(long minTranslogGeneration) {
        this.minTranslogGeneration = minTranslogGeneration;
    }

    @Override
    public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
        return translogCheckpointFileInfoTupleSet.stream().map(Tuple::v1).collect(Collectors.toSet());
    }

    @Override
    public TranslogTransferMetadata getTranslogTransferMetadata() {
        return new TranslogTransferMetadata(
            primaryTerm,
            generation,
            minTranslogGeneration,
            translogCheckpointFileInfoTupleSet.size() * 2,
            nodeId
        );
    }

    @Override
    public Set<TranslogCheckpointSnapshot> getTranslogCheckpointSnapshots() {
        return translogCheckpointSnapshotSet;
    }

    @Override
    public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
        return translogCheckpointFileInfoTupleSet.stream().map(Tuple::v2).collect(Collectors.toSet());
    }

    public void close() throws IOException {
        List<Exception> exceptionList = new ArrayList<>();
        Set<FileSnapshot.TransferFileSnapshot> fileSnapshots = getTranslogFileSnapshots();
        fileSnapshots.addAll(getCheckpointFileSnapshots());

        for (FileSnapshot fileSnapshot : fileSnapshots) {
            try {
                fileSnapshot.close();
            } catch (IOException e) {
                exceptionList.add(e);
            }
        }

        if (!exceptionList.isEmpty()) {
            IOException ex = new IOException("IO Exception while closing file snapshots");
            exceptionList.forEach(ex::addSuppressed);
            throw ex;
        }
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

    /**
     * Builder for {@link TranslogCheckpointTransferSnapshot}
     */
    public static class Builder {
        private final long primaryTerm;
        private final long generation;
        private final List<TranslogReader> readers;
        private final Function<Long, String> checkpointGenFileNameMapper;
        private final Path location;
        private final String nodeId;

        public Builder(
            long primaryTerm,
            long generation,
            Path location,
            List<TranslogReader> readers,
            Function<Long, String> checkpointGenFileNameMapper,
            String nodeId
        ) {
            this.primaryTerm = primaryTerm;
            this.generation = generation;
            this.readers = readers;
            this.checkpointGenFileNameMapper = checkpointGenFileNameMapper;
            this.location = location;
            this.nodeId = nodeId;
        }

        public TranslogCheckpointTransferSnapshot build() throws IOException {
            final List<Long> generations = new LinkedList<>();
            long highestGeneration = Long.MIN_VALUE;
            long highestGenPrimaryTerm = Long.MIN_VALUE;
            long lowestGeneration = Long.MAX_VALUE;
            long highestGenMinTranslogGeneration = Long.MIN_VALUE;
            TranslogCheckpointTransferSnapshot translogTransferSnapshot = new TranslogCheckpointTransferSnapshot(
                primaryTerm,
                generation,
                readers.size(),
                nodeId
            );
            for (TranslogReader reader : readers) {
                final long readerGeneration = reader.getGeneration();
                final long readerPrimaryTerm = reader.getPrimaryTerm();
                final long minTranslogGeneration = reader.getCheckpoint().getMinTranslogGeneration();
                final long checkpointGeneration = reader.getCheckpoint().getGeneration();
                Path translogPath = reader.path();
                Path checkpointPath = location.resolve(checkpointGenFileNameMapper.apply(readerGeneration));
                generations.add(readerGeneration);
                translogTransferSnapshot.add(
                    new TranslogFileSnapshot(readerPrimaryTerm, readerGeneration, translogPath, reader.getTranslogChecksum()),
                    new CheckpointFileSnapshot(
                        readerPrimaryTerm,
                        checkpointGeneration,
                        minTranslogGeneration,
                        checkpointPath,
                        reader.getCheckpointChecksum()
                    ),
                    new TranslogCheckpointSnapshot(
                        readerPrimaryTerm,
                        readerGeneration,
                        minTranslogGeneration,
                        translogPath,
                        checkpointPath,
                        reader.getTranslogChecksum(),
                        reader.getCheckpointChecksum(),
                        reader.getCheckpoint()
                    )
                );
                if (readerGeneration > highestGeneration) {
                    highestGeneration = readerGeneration;
                    highestGenMinTranslogGeneration = minTranslogGeneration;
                    highestGenPrimaryTerm = readerPrimaryTerm;
                }
                lowestGeneration = Math.min(lowestGeneration, readerGeneration);
            }
            translogTransferSnapshot.setMinTranslogGeneration(highestGenMinTranslogGeneration);

            assert this.primaryTerm == highestGenPrimaryTerm : "inconsistent primary term";
            assert this.generation == highestGeneration : " inconsistent generation ";
            final long finalHighestGeneration = highestGeneration;
            assert LongStream.iterate(lowestGeneration, i -> i + 1)
                .limit(highestGeneration)
                .filter(l -> (l <= finalHighestGeneration))
                .boxed()
                .collect(Collectors.toList())
                .equals(generations.stream().sorted().collect(Collectors.toList())) == true : "generation gaps found";
            return translogTransferSnapshot;
        }
    }
}
