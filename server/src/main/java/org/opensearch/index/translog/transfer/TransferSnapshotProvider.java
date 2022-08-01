/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.translog.FileInfo;
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

    private final Snapshot translogTransferSnapshot;

    public TransferSnapshotProvider(Path location, List<TranslogReader> readers) throws IOException {
        translogTransferSnapshot = new Snapshot(readers.size());
        for (TranslogReader reader : readers) {
            final long generation = reader.getGeneration();
            Path translogPath = reader.path();
            Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(generation));
            translogTransferSnapshot.add(buildFileInfo(translogPath.toFile()), buildFileInfo(checkpointPath.toFile()));
        }
    }

    public Snapshot get() {
        return translogTransferSnapshot.verify() ? translogTransferSnapshot : null;
    }

    private FileInfo buildFileInfo(File file) throws IOException {
        FileInfo fileInfo;
        try (CheckedInputStream stream = new CheckedInputStream(new FileInputStream(file), new CRC32())) {
            byte[] content = stream.readAllBytes();
            long checksum = stream.getChecksum().getValue();
            fileInfo = new FileInfo(file.getName(), file.toPath(), checksum, content);
        }
        return fileInfo;
    }

    static class Snapshot implements TransferSnapshot {

        private final Set<Tuple<FileInfo, FileInfo>> translogCheckpointFileInfoTupleSet;
        private final int size;

        Snapshot(int size) {
            translogCheckpointFileInfoTupleSet = new HashSet<>(size);
            this.size = size;
        }

        private void add(FileInfo translogFileInfo, FileInfo checkPointFileInfo) {
            translogCheckpointFileInfoTupleSet.add(Tuple.tuple(translogFileInfo, checkPointFileInfo));
        }

        private boolean verify() {
            return translogCheckpointFileInfoTupleSet.size() == size;
        }

        public Set<FileInfo> getTranslogFileInfos() {
            return translogCheckpointFileInfoTupleSet.stream().map(tuple -> tuple.v1()).collect(Collectors.toSet());
        }

        public Set<FileInfo> getCheckpointFileInfos() {
            return translogCheckpointFileInfoTupleSet.stream().map(tuple -> tuple.v2()).collect(Collectors.toSet());
        }
    }
}
