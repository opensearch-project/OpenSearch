/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CompositeEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ExperimentalApi
public class IndexFileDeleter {

    private final Map<DataFormat, Map<String, AtomicInteger>> fileRefCounts = new ConcurrentHashMap<>();
    private final CompositeEngine compositeEngine;

    public IndexFileDeleter(CompositeEngine compositeEngine, CatalogSnapshot initialCatalogSnapshot, ShardPath shardPath)
        throws IOException {
        this.compositeEngine = compositeEngine;
        if (initialCatalogSnapshot != null) {
            addFileReferences(initialCatalogSnapshot);
            deleteUnreferencedFiles(shardPath);
        }
    }

    public synchronized void addFileReferences(CatalogSnapshot snapshot) {
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
                    // First reference — this file is new
                    newFiles.add(file);
                }
            }
            if (!newFiles.isEmpty()) {
                dfNewFiles.put(dataFormat, newFiles);
            }
        }

        if (!dfNewFiles.isEmpty()) {
            notifyFilesAdded(dfNewFiles);
        }
    }

    public synchronized void removeFileReferences(CatalogSnapshot snapshot) {
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
            if (!filesToDelete.isEmpty()) {
                dfFilesToDelete.put(dataFormat, filesToDelete);
            }
        }

        if (!dfFilesToDelete.isEmpty()) {
            notifyFilesDeleted(dfFilesToDelete);
        }
    }

    private void notifyFilesAdded(Map<DataFormat, Collection<String>> dfNewFiles) {
        try {
            compositeEngine.notifyFilesAdded(dfNewFiles);
        } catch (Exception e) {
            System.err.println("Failed to notify new files: " + dfNewFiles + ", error: " + e.getMessage());
        }
    }

    private void notifyFilesDeleted(Map<DataFormat, Collection<String>> dfFilesToDelete) {
        try {
            compositeEngine.notifyDelete(dfFilesToDelete);
        } catch (Exception e) {
            System.err.println("Failed to delete unreferenced files: " + dfFilesToDelete + ", error: " + e.getMessage());
        }
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
