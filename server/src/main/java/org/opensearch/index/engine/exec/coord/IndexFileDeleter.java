/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks per-format file reference counts and coordinates file deletion
 * for pluggable engines. Analogous to Lucene's internal IndexFileDeleter
 * but driven by {@link CatalogSnapshot} lifecycle instead of Lucene commits.
 * <p>
 * File ref counts are managed at the CatalogSnapshot level:
 * <ul>
 *   <li>{@link #addFileReferences} is called when a CatalogSnapshot is created</li>
 *   <li>{@link #removeFileReferences} is called when a CatalogSnapshot's refCount reaches 0</li>
 * </ul>
 * <p>
 * CatalogSnapshot refCount is managed by:
 * <ul>
 *   <li>The manager (holds one ref as "latest", releases on refresh)</li>
 *   <li>Readers (incRef on acquire, decRef on close)</li>
 *   <li>Commits (incRef on commit, decRef when deletion policy deletes the commit)</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFileDeleter {

    private final Map<String, Map<String, AtomicInteger>> fileRefCounts;
    private final CatalogSnapshotDeletionPolicy deletionPolicy;
    private final Map<String, FileDeleter> fileDeleters;
    private final Map<String, FilesListener> filesListeners;
    private final List<CatalogSnapshot> committedSnapshots;

    public IndexFileDeleter(
        CatalogSnapshotDeletionPolicy deletionPolicy,
        Map<String, FileDeleter> fileDeleters,
        Map<String, FilesListener> filesListeners,
        CatalogSnapshot committedSnapshot,
        ShardPath shardPath
    ) throws IOException {
        this.deletionPolicy = deletionPolicy;
        this.fileDeleters = fileDeleters;
        this.filesListeners = filesListeners;
        this.fileRefCounts = new HashMap<>();
        this.committedSnapshots = new ArrayList<>();

        // incRef for the commit (manager already holds one ref as "latest")
        committedSnapshot.tryIncRef();
        committedSnapshots.add(committedSnapshot);
        addFileReferences(committedSnapshot);

        List<CatalogSnapshot> toDelete = deletionPolicy.onInit(committedSnapshots);
        for (CatalogSnapshot old : toDelete) {
            committedSnapshots.remove(old);
            if (old.decRef()) {
                removeFileReferences(old);
            }
        }

        deleteOrphanedFiles(shardPath);
    }

    // ---- File ref counting ----

    /**
     * Called when a CatalogSnapshot is created.
     * Increments ref counts for all its files.
     *
     * @return files whose ref count went from 0 to 1 (newly visible), grouped by format name
     */
    public synchronized Map<String, Collection<String>> addFileReferences(CatalogSnapshot snapshot) throws IOException {
        Map<String, Collection<String>> segregated = segregateFilesByFormat(snapshot);
        Map<String, Collection<String>> newFiles = new HashMap<>();

        for (Map.Entry<String, Collection<String>> entry : segregated.entrySet()) {
            String formatName = entry.getKey();
            Collection<String> added = new HashSet<>();
            Map<String, AtomicInteger> formatRefs = fileRefCounts.computeIfAbsent(formatName, k -> new HashMap<>());

            for (String file : entry.getValue()) {
                AtomicInteger refCount = formatRefs.computeIfAbsent(file, k -> new AtomicInteger(0));
                if (refCount.incrementAndGet() == 1) {
                    added.add(file);
                }
            }
            if (added.isEmpty() == false) {
                newFiles.put(formatName, added);
            }
        }
        notifyFilesAdded(newFiles);
        return newFiles;
    }

    /**
     * Called when a CatalogSnapshot's refCount reaches 0.
     * Decrements ref counts for its files and deletes files that hit 0.
     */
    public synchronized void removeFileReferences(CatalogSnapshot snapshot) throws IOException {
        Map<String, Collection<String>> filesToDelete = decRefFiles(snapshot);
        if (filesToDelete.isEmpty() == false) {
            deleteFiles(filesToDelete);
            notifyFilesDeleted(filesToDelete);
        }
    }

    // ---- Commit path ----

    /**
     * A CatalogSnapshot has been committed (flushed). The commit ref is the incRef
     * from {@code acquireSnapshotForCommit()} — this method does not incRef again.
     * Adds to the committed list, then delegates to the deletion policy.
     * Snapshots the policy wants to delete get their commit ref decRef'd,
     * which may trigger removeFileReferences when their refCount reaches 0.
     */
    public synchronized void onCommit(CatalogSnapshot committedSnapshot) throws IOException {
        committedSnapshots.add(committedSnapshot);

        List<CatalogSnapshot> toDelete = deletionPolicy.onCommit(committedSnapshots);

        for (CatalogSnapshot old : toDelete) {
            committedSnapshots.remove(old);
            if (old.decRef()) {
                removeFileReferences(old);
            }
        }
    }

    // ---- Policy revisit ----

    /**
     * Re-runs the deletion policy against the current committed snapshots list.
     * Used after releasing a snapshot hold — the policy may now allow
     * deletion of commits it previously protected.
     */
    public synchronized void revisitPolicy() throws IOException {
        List<CatalogSnapshot> toDelete = deletionPolicy.onCommit(committedSnapshots);
        for (CatalogSnapshot old : toDelete) {
            committedSnapshots.remove(old);
            if (old.decRef()) {
                removeFileReferences(old);
            }
        }
    }

    // ---- Listener notification ----

    private void deleteFiles(Map<String, Collection<String>> filesByFormat) throws IOException {
        for (Map.Entry<String, Collection<String>> entry : filesByFormat.entrySet()) {
            FileDeleter deleter = fileDeleters.get(entry.getKey());
            if (deleter != null) {
                deleter.deleteFiles(Map.of(entry.getKey(), entry.getValue()));
            }
        }
    }

    private void notifyFilesAdded(Map<String, Collection<String>> newFilesByFormat) throws IOException {
        for (Map.Entry<String, Collection<String>> entry : newFilesByFormat.entrySet()) {
            FilesListener listener = filesListeners.get(entry.getKey());
            if (listener != null) {
                listener.onFilesAdded(entry.getValue());
            }
        }
    }

    private void notifyFilesDeleted(Map<String, Collection<String>> deletedFilesByFormat) throws IOException {
        for (Map.Entry<String, Collection<String>> entry : deletedFilesByFormat.entrySet()) {
            FilesListener listener = filesListeners.get(entry.getKey());
            if (listener != null) {
                listener.onFilesDeleted(entry.getValue());
            }
        }
    }

    // ---- Internal ----

    private Map<String, Collection<String>> decRefFiles(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> segregated = segregateFilesByFormat(snapshot);
        Map<String, Collection<String>> filesToDelete = new HashMap<>();

        for (Map.Entry<String, Collection<String>> entry : segregated.entrySet()) {
            String formatName = entry.getKey();
            Collection<String> toDelete = new HashSet<>();
            Map<String, AtomicInteger> formatRefs = fileRefCounts.get(formatName);

            if (formatRefs != null) {
                for (String file : entry.getValue()) {
                    AtomicInteger refCount = formatRefs.get(file);
                    if (refCount != null && refCount.decrementAndGet() == 0) {
                        formatRefs.remove(file);
                        toDelete.add(file);
                    }
                }
            }
            if (toDelete.isEmpty() == false) {
                filesToDelete.put(formatName, toDelete);
            }
        }
        return filesToDelete;
    }

    private Map<String, Collection<String>> segregateFilesByFormat(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> result = new HashMap<>();
        for (var segment : snapshot.getSegments()) {
            for (var entry : segment.dfGroupedSearchableFiles().entrySet()) {
                result.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).addAll(entry.getValue().files());
            }
        }
        return result;
    }

    private void deleteOrphanedFiles(ShardPath shardPath) throws IOException {
        if (shardPath == null) {
            return;
        }
        Map<String, Collection<String>> orphansByFormat = new HashMap<>();
        for (Map.Entry<String, Map<String, AtomicInteger>> entry : fileRefCounts.entrySet()) {
            String formatName = entry.getKey();
            Set<String> knownFiles = entry.getValue().keySet();
            Path formatDir = shardPath.getDataPath().resolve(formatName);
            if (Files.exists(formatDir) == false) {
                continue;
            }
            Collection<String> orphans = new HashSet<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(formatDir)) {
                for (Path file : stream) {
                    if (Files.isRegularFile(file) && knownFiles.contains(file.getFileName().toString()) == false) {
                        orphans.add(file.getFileName().toString());
                    }
                }
            }
            if (orphans.isEmpty() == false) {
                orphansByFormat.put(formatName, orphans);
            }
        }
        if (orphansByFormat.isEmpty() == false) {
            deleteFiles(orphansByFormat);
        }
    }

    @Override
    public String toString() {
        return "IndexFileDeleter{fileRefCounts=" + fileRefCounts + ", committedSnapshots=" + committedSnapshots.size() + "}";
    }
}
