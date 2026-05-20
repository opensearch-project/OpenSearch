/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.exec.CommitFileManager;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Scans format directories on disk and identifies files that are not referenced
 * by any known snapshot and not managed by the commit mechanism.
 * <p>
 * Stateless utility — does not hold any mutable state. Called once during
 * {@link IndexFileDeleter} construction to clean up files left behind by
 * a crash between ref-count decrement and physical deletion.
 */
final class OrphanFileScanner {

    private OrphanFileScanner() {}

    /**
     * Finds orphaned files across all known format directories.
     *
     * @param shardPath        the shard's data path (null skips the scan)
     * @param knownFilesByFormat map of format name to the set of file names currently tracked
     * @param commitFileManager optional manager that identifies commit-owned files (may be null)
     * @return map of format name to orphaned file names; empty if nothing to clean up
     */
    static Map<String, Collection<String>> findOrphans(
        ShardPath shardPath,
        Map<String, Set<String>> knownFilesByFormat,
        CommitFileManager commitFileManager
    ) throws IOException {
        if (shardPath == null) {
            return Map.of();
        }
        Map<String, Collection<String>> orphansByFormat = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : knownFilesByFormat.entrySet()) {
            String formatName = entry.getKey();
            Set<String> knownFiles = entry.getValue();
            Path formatDir = "lucene".equals(formatName) ? shardPath.resolveIndex() : shardPath.getDataPath().resolve(formatName);
            if (Files.exists(formatDir) == false) {
                continue;
            }
            Collection<String> orphans = new HashSet<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(formatDir)) {
                for (Path file : stream) {
                    String fileName = file.getFileName().toString();
                    if (Files.isRegularFile(file)
                        && knownFiles.contains(fileName) == false
                        && (commitFileManager == null || commitFileManager.isCommitManagedFile(fileName) == false)) {
                        orphans.add(fileName);
                    }
                }
            }
            if (orphans.isEmpty() == false) {
                orphansByFormat.put(formatName, orphans);
            }
        }
        return orphansByFormat;
    }
}
