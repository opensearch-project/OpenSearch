/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.FileInfo;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class TranslogUploadSnapshot {

    private List<FileInfo> fileInfos;

    public TranslogUploadSnapshot(Path location, List<TranslogReader> readers) throws IOException {
        this.fileInfos = new ArrayList<>(readers.size());
        for (TranslogReader reader : readers) {
            final long generation = reader.getGeneration();
            Path translogPath = reader.path();
            fileInfos.add(buildFileInfo(translogPath.toFile()));
            Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(generation));
            fileInfos.add(buildFileInfo(checkpointPath.toFile()));
        }
    }

    public List<FileInfo> getSnapshot() {
        return Collections.unmodifiableList(fileInfos);
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
}
