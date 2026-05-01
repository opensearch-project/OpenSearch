/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CommitFileManager;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
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

    private static final Logger logger = LogManager.getLogger(IndexFileDeleter.class);

    private final Map<String, Map<String, AtomicInteger>> fileRefCounts;
    private final CatalogSnapshotDeletionPolicy deletionPolicy;
    private final FileDeleter fileDeleter;
    private final Map<String, FilesListener> filesListeners;
    private final List<CatalogSnapshot> committedSnapshots;
    private final CommitFileManager commitFileManager;

    /**
     * Files that were dereferenced (ref count hit 0) but whose physical deletion
     * failed. Retried on the next {@link #removeFileReferences}, {@link #revisitPolicy},
     * or {@link #retryPendingDeletes} call.
     */
    private final Map<String, Set<String>> pendingDeletes;

    public IndexFileDeleter(
        CatalogSnapshotDeletionPolicy deletionPolicy,
        FileDeleter fileDeleter,
        Map<String, FilesListener> filesListeners,
        List<CatalogSnapshot> initialCommittedSnapshots,
        ShardPath shardPath,
        CommitFileManager commitFileManager
    ) throws IOException {
        this.deletionPolicy = deletionPolicy;
        this.fileDeleter = fileDeleter;
        this.filesListeners = filesListeners;
        this.fileRefCounts = new HashMap<>();
        this.committedSnapshots = new ArrayList<>();
        this.commitFileManager = commitFileManager;
        this.pendingDeletes = new HashMap<>();

        for (CatalogSnapshot cs : initialCommittedSnapshots) {
            if (cs.tryIncRef() == false) {
                throw new IllegalStateException("Committed snapshot [gen=" + cs.getGeneration() + "] is already closed");
            }
            cs.markCommitted();
            this.committedSnapshots.add(cs);
            addFileReferences(cs);
        }

        // No synchronization needed — `this` hasn't escaped the constructor yet.
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
        Map<String, Collection<String>> segregated = snapshot.getFilesByFormat();
        Map<String, Collection<String>> newFiles = new HashMap<>();

        for (Map.Entry<String, Collection<String>> entry : segregated.entrySet()) {
            String formatName = entry.getKey();
            Collection<String> added = new HashSet<>();
            Map<String, AtomicInteger> formatRefs = fileRefCounts.computeIfAbsent(formatName, k -> new HashMap<>());

            for (String file : entry.getValue()) {
                AtomicInteger refCount = formatRefs.computeIfAbsent(file, k -> new AtomicInteger(0));
                if (refCount.incrementAndGet() == 1) {
                    added.add(file);
                    assert pendingDeletes.getOrDefault(formatName, Set.of()).contains(file) == false : "File ["
                        + file
                        + "] in format ["
                        + formatName
                        + "] was pending deletion but is being re-referenced."
                        + " This should never happen — once a segment file's ref count reaches 0, no new snapshot should reference it.";
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
     * <p>
     * Ref count bookkeeping is done under the lock; actual I/O (commit deletion,
     * file deletion, listener notification) is performed outside the lock to avoid
     * blocking concurrent {@link #addFileReferences} and {@link #onCommit} calls.
     */
    public void removeFileReferences(CatalogSnapshot snapshot) throws IOException {
        Map<String, Collection<String>> filesToDelete;
        synchronized (this) {
            filesToDelete = decRefFiles(snapshot);
        }
        // Delete the commit point (segments_N) BEFORE deleting data files,
        // because deleteCommit may call DirectoryReader.listCommits() which
        // needs to read segment files that are about to be deleted.
        if (commitFileManager != null && snapshot.isCommitted()) {
            commitFileManager.deleteCommit(snapshot);
        }
        if (filesToDelete.isEmpty() == false) {
            executeDeletesWithRetry(filesToDelete);
        }
        // Also retry any previously failed deletes
        retryPendingDeletes();
    }

    // ---- Commit path ----

    /**
     * A CatalogSnapshot has been committed (flushed). The commit ref is the incRef
     * from {@code acquireSnapshotForCommit()} — this method does not incRef again.
     * Adds to the committed list, then delegates to the deletion policy.
     * Snapshots the policy wants to delete get their commit ref decRef'd,
     * which may trigger removeFileReferences when their refCount reaches 0.
     */
    public void onCommit(CatalogSnapshot committedSnapshot) throws IOException {
        List<CatalogSnapshot> toDelete;
        synchronized (this) {
            committedSnapshots.add(committedSnapshot);
            toDelete = deletionPolicy.onCommit(committedSnapshots);
            for (CatalogSnapshot old : toDelete) {
                committedSnapshots.remove(old);
            }
        }

        for (CatalogSnapshot old : toDelete) {
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
    public void revisitPolicy() throws IOException {
        List<CatalogSnapshot> toDelete;
        synchronized (this) {
            toDelete = deletionPolicy.onCommit(committedSnapshots);
            for (CatalogSnapshot old : toDelete) {
                committedSnapshots.remove(old);
            }
        }
        for (CatalogSnapshot old : toDelete) {
            if (old.decRef()) {
                removeFileReferences(old);
            }
        }
    }

    /**
     * Retries deletion of files that failed to delete on a previous attempt.
     * Can be called periodically or after operations that may have released
     * filesystem locks.
     */
    public void retryPendingDeletes() throws IOException {
        Map<String, Set<String>> snapshot;
        synchronized (this) {
            if (pendingDeletes.isEmpty()) {
                return;
            }
            // Take a snapshot of pending deletes to process outside the lock
            snapshot = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : pendingDeletes.entrySet()) {
                snapshot.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
        }
        Map<String, Collection<String>> allDeleted = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : snapshot.entrySet()) {
            String formatName = entry.getKey();
            Set<String> files = entry.getValue();
            Set<String> stillFailed = new HashSet<>();
            for (String file : files) {
                // Assert: a file in pendingDeletes must not be re-referenced
                synchronized (this) {
                    Map<String, AtomicInteger> formatRefs = fileRefCounts.get(formatName);
                    assert (formatRefs == null || formatRefs.containsKey(file) == false) : "File ["
                        + file
                        + "] in format ["
                        + formatName
                        + "] is pending deletion but has a live ref count."
                        + " This should never happen — once a segment file's ref count reaches 0, no new snapshot should reference it.";
                }
                try {
                    Map<String, Collection<String>> failed = AccessController.doPrivilegedChecked(
                        () -> fileDeleter.deleteFiles(Map.of(formatName, List.of(file)))
                    );
                    if (failed.getOrDefault(formatName, Set.of()).contains(file)) {
                        stillFailed.add(file);
                    } else {
                        allDeleted.computeIfAbsent(formatName, k -> new HashSet<>()).add(file);
                    }
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("Retry delete failed for [{}] in format [{}]", file, formatName), e);
                    stillFailed.add(file);
                }
            }
            synchronized (this) {
                Set<String> currentPending = pendingDeletes.get(formatName);
                if (currentPending != null) {
                    currentPending.retainAll(stillFailed);
                    if (currentPending.isEmpty()) {
                        pendingDeletes.remove(formatName);
                    }
                }
            }
        }
        if (allDeleted.isEmpty() == false) {
            notifyFilesDeleted(allDeleted);
        }
    }

    // ---- Listener notification (outside lock) ----

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

    // ---- Deletion with pending-delete tracking ----

    /**
     * Attempts to delete files. Before deleting, re-checks under the lock that each file
     * is still unreferenced — a concurrent {@link #addFileReferences} may have re-referenced
     * a file between {@link #decRefFiles} and this call. Files that fail to delete are added
     * to {@link #pendingDeletes} for retry. Successfully deleted files trigger listener notification.
     */
    private void executeDeletesWithRetry(Map<String, Collection<String>> filesByFormat) throws IOException {
        // Filter out files that were re-referenced since decRefFiles ran
        Map<String, Collection<String>> safeToDelete = new HashMap<>();
        synchronized (this) {
            for (Map.Entry<String, Collection<String>> entry : filesByFormat.entrySet()) {
                String formatName = entry.getKey();
                Map<String, AtomicInteger> formatRefs = fileRefCounts.get(formatName);
                Collection<String> stillDead = new HashSet<>();
                for (String file : entry.getValue()) {
                    if (formatRefs == null || formatRefs.containsKey(file) == false) {
                        stillDead.add(file);
                    }
                }
                if (stillDead.isEmpty() == false) {
                    safeToDelete.put(formatName, stillDead);
                }
            }
        }
        Map<String, Collection<String>> successfullyDeleted = new HashMap<>();
        for (Map.Entry<String, Collection<String>> entry : safeToDelete.entrySet()) {
            String formatName = entry.getKey();
            Collection<String> files = entry.getValue();
            if (fileDeleter != null) {
                try {
                    Map<String, Collection<String>> failed = AccessController.doPrivilegedChecked(
                        () -> fileDeleter.deleteFiles(Map.of(formatName, files))
                    );
                    Collection<String> failedForFormat = failed.getOrDefault(formatName, Set.of());
                    if (failedForFormat.isEmpty() == false) {
                        synchronized (this) {
                            pendingDeletes.computeIfAbsent(formatName, k -> new HashSet<>()).addAll(failedForFormat);
                        }
                    }
                    Collection<String> succeeded = new HashSet<>(files);
                    succeeded.removeAll(failedForFormat);
                    if (succeeded.isEmpty() == false) {
                        successfullyDeleted.put(formatName, succeeded);
                    }
                } catch (IOException e) {
                    logger.warn(
                        () -> new ParameterizedMessage("Failed to delete files for format [{}], adding to pending deletes", formatName),
                        e
                    );
                    synchronized (this) {
                        pendingDeletes.computeIfAbsent(formatName, k -> new HashSet<>()).addAll(files);
                    }
                }
            }
        }
        if (successfullyDeleted.isEmpty() == false) {
            notifyFilesDeleted(successfullyDeleted);
        }
    }

    // ---- Internal ----

    private Map<String, Collection<String>> decRefFiles(CatalogSnapshot snapshot) {
        assert Thread.holdsLock(this);
        Map<String, Collection<String>> segregated = snapshot.getFilesByFormat();
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

    /**
     * Scans format directories for files not tracked by any snapshot and deletes them.
     * Called once during construction to clean up after a crash between ref-count
     * decrement and physical deletion.
     */
    private void deleteOrphanedFiles(ShardPath shardPath) throws IOException {
        Map<String, Set<String>> knownFilesByFormat = new HashMap<>();
        for (Map.Entry<String, Map<String, AtomicInteger>> entry : fileRefCounts.entrySet()) {
            knownFilesByFormat.put(entry.getKey(), new HashSet<>(entry.getValue().keySet()));
        }
        Map<String, Collection<String>> orphans = OrphanFileScanner.findOrphans(shardPath, knownFilesByFormat, commitFileManager);
        if (orphans.isEmpty() == false) {
            executeDeletesWithRetry(orphans);
        }
    }

    /**
     * Returns true if there are files awaiting deletion retry.
     * Visible for testing.
     */
    public synchronized boolean hasPendingDeletes() {
        return pendingDeletes.isEmpty() == false;
    }

    /**
     * Returns a snapshot of the current pending deletes map.
     * Visible for testing.
     */
    synchronized Map<String, Set<String>> getPendingDeletes() {
        Map<String, Set<String>> copy = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : pendingDeletes.entrySet()) {
            copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return copy;
    }

    /**
     * Returns a string representation of this IndexFileDeleter.
     */
    @Override
    public String toString() {
        return "IndexFileDeleter{fileRefCounts=" + fileRefCounts + ", committedSnapshots=" + committedSnapshots.size() + "}";
    }
}
