/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks per-format file reference counts and computes which files are newly
 * added or fully dereferenced after catalog snapshot changes.
 * <p>
 * This class does <em>not</em> notify reader managers itself — it returns the
 * computed change sets so the caller ({@link DataFormatAwareEngine})
 * can route notifications to the appropriate reader managers.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFileDeleter {

    private final Map<DataFormat, Map<String, AtomicInteger>> fileRefCounts = new ConcurrentHashMap<>();

    public IndexFileDeleter(CatalogSnapshot initialCatalogSnapshot, ShardPath shardPath) throws IOException {
        if (initialCatalogSnapshot != null) {
            addFileReferences(initialCatalogSnapshot);
            deleteUnreferencedFiles(shardPath);
        }
    }

    /**
     * Increments reference counts for all files in the snapshot.
     *
     * @return files whose reference count went from 0 → 1 (newly added), grouped by format.
     *         Returns an empty map when there are no new files.
     */
    public synchronized Map<DataFormat, Collection<String>> addFileReferences(CatalogSnapshot snapshot) {
        Map<DataFormat, Collection<String>> dfSegregatedFiles = segregateFilesByFormat(snapshot);
        Map<DataFormat, Collection<String>> dfNewFiles = new HashMap<>();

        for (Map.Entry<DataFormat, Collection<String>> entry : dfSegregatedFiles.entrySet()) {
            DataFormat dataFormat = entry.getKey();
            Collection<String> newFiles = new HashSet<>();
            Map<String, AtomicInteger> dfFileRefCounts = fileRefCounts.computeIfAbsent(dataFormat, k -> new HashMap<>());
            Collection<String> files = entry.getValue();
            for (String file : files) {
                AtomicInteger refCount = dfFileRefCounts.computeIfAbsent(file, k -> new AtomicInteger(0));
                if (refCount.incrementAndGet() == 1) {
                    newFiles.add(file);
                }
            }
            if (newFiles.isEmpty() == false) {
                dfNewFiles.put(dataFormat, newFiles);
            }
        }

        return dfNewFiles.isEmpty() ? Collections.emptyMap() : dfNewFiles;
    }

    /**
     * Decrements reference counts for all files in the snapshot.
     *
     * @return files whose reference count reached 0 (ready for deletion), grouped by format.
     *         Returns an empty map when there are no files to delete.
     */
    public synchronized Map<DataFormat, Collection<String>> removeFileReferences(CatalogSnapshot snapshot) {
        Map<DataFormat, Collection<String>> dfSegregatedFiles = segregateFilesByFormat(snapshot);
        Map<DataFormat, Collection<String>> dfFilesToDelete = new HashMap<>();

        for (Map.Entry<DataFormat, Collection<String>> entry : dfSegregatedFiles.entrySet()) {
            DataFormat dataFormat = entry.getKey();
            Collection<String> filesToDelete = new HashSet<>();
            Map<String, AtomicInteger> dfFileRefCounts = fileRefCounts.get(dataFormat);
            if (dfFileRefCounts != null) {
                Collection<String> files = entry.getValue();
                for (String file : files) {
                    AtomicInteger refCount = dfFileRefCounts.get(file);
                    if (refCount != null && refCount.decrementAndGet() == 0) {
                        dfFileRefCounts.remove(file);
                        filesToDelete.add(file);
                    }
                }
            }
            if (filesToDelete.isEmpty() == false) {
                dfFilesToDelete.put(dataFormat, filesToDelete);
            }
        }

        return dfFilesToDelete.isEmpty() ? Collections.emptyMap() : dfFilesToDelete;
    }

    private Map<DataFormat, Collection<String>> segregateFilesByFormat(CatalogSnapshot snapshot) {
        Map<DataFormat, Collection<String>> dfSegregatedFiles = new HashMap<>();
        // TODO
        return dfSegregatedFiles;
    }

    private void deleteUnreferencedFiles(ShardPath shardPath) throws IOException {
        // TODO
    }

    @Override
    public String toString() {
        return "IndexFileDeleter{fileRefCounts=" + fileRefCounts + "}";
    }
}
