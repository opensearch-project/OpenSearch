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
 * <p>
 * Keys are <em>storage</em> names, not catalog format names: this scans directories, and
 * several catalog formats can share one (a side table's files sit beside its delegate's).
 * A caller that passed catalog names would have each format see only its own files in a
 * shared directory and delete every other format's as orphaned. See
 * {@link IndexFileDeleter#deleteOrphanedFiles}, which does the grouping.
 */
final class OrphanFileScanner {

    private OrphanFileScanner() {}

    /**
     * Finds orphaned files across all known format directories.
     *
     * @param shardPath        the shard's data path (null skips the scan)
     * @param knownFilesByStorage map of <em>storage</em> name to every file name currently tracked
     *                            in that storage, pooled across the catalog formats sharing it
     * @param commitFileManager optional manager that identifies commit-owned files (may be null)
     * @return map of storage name to orphaned file names; empty if nothing to clean up
     */
    static Map<String, Collection<String>> findOrphans(
        ShardPath shardPath,
        Map<String, Set<String>> knownFilesByStorage,
        CommitFileManager commitFileManager
    ) throws IOException {
        if (shardPath == null) {
            return Map.of();
        }
        Map<String, Collection<String>> orphansByFormat = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : knownFilesByStorage.entrySet()) {
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
