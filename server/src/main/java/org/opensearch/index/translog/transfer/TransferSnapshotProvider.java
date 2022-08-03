/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.translog.FileSnapshot;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class TransferSnapshotProvider implements Supplier<TransferSnapshot> {

    private final TranslogCheckpointTransferSnapshot translogTransferSnapshot;

    public TransferSnapshotProvider(Path location, List<TranslogReader> readers) throws IOException {
        translogTransferSnapshot = new TranslogCheckpointTransferSnapshot(readers.size());
        for (TranslogReader reader : readers) {
            final long generation = reader.getGeneration();
            Path translogPath = reader.path();
            Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(generation));
            translogTransferSnapshot.add(buildFileInfo(translogPath.toFile()), buildFileInfo(checkpointPath.toFile()));
        }
    }

    public TranslogCheckpointTransferSnapshot get() {
        return translogTransferSnapshot.verify() ? translogTransferSnapshot : null;
    }

    private FileSnapshot buildFileInfo(File file) throws IOException {
        FileSnapshot fileSnapshot;
        try (CheckedInputStream stream = new CheckedInputStream(new FileInputStream(file), new CRC32())) {
            byte[] content = stream.readAllBytes();
            long checksum = stream.getChecksum().getValue();
            fileSnapshot = new FileSnapshot(file.getName(), file.toPath(), checksum, content);
        }
        return fileSnapshot;
    }

    static class TranslogCheckpointTransferSnapshot implements TransferSnapshot {

        private final Set<Tuple<FileSnapshot, FileSnapshot>> translogCheckpointFileInfoTupleSet;
        private final int size;

        TranslogCheckpointTransferSnapshot(int size) {
            translogCheckpointFileInfoTupleSet = new HashSet<>(size);
            this.size = size;
        }

        private void add(FileSnapshot translogFileSnapshot, FileSnapshot checkPointFileSnapshot) {
            translogCheckpointFileInfoTupleSet.add(Tuple.tuple(translogFileSnapshot, checkPointFileSnapshot));
        }

        private boolean verify() {
            return translogCheckpointFileInfoTupleSet.size() == size;
        }

        public Set<FileSnapshot> getTranslogFileSnapshots() {
            return translogCheckpointFileInfoTupleSet.stream().map(tuple -> tuple.v1()).collect(Collectors.toSet());
        }

        public Set<FileSnapshot> getCheckpointFileSnapshots() {
            return translogCheckpointFileInfoTupleSet.stream().map(tuple -> tuple.v2()).collect(Collectors.toSet());
        }

        @Override
        public int getTransferSize() {
            return 2 * size;
        }
    }
}
