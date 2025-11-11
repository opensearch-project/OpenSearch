/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@ExperimentalApi
public class IndexFileDeleter {

    private final Map<String,Map<String, AtomicInteger>> fileRefCounts = new ConcurrentHashMap<>();
    private final CompositeIndexingExecutionEngine engine;

    public IndexFileDeleter(CompositeIndexingExecutionEngine engine, CatalogSnapshot initialCatalogSnapshot, ShardPath shardPath) throws IOException {
        this.engine = engine;
        if (initialCatalogSnapshot != null) {
            addFileReferences(initialCatalogSnapshot);
            deleteUnreferencedFiles(shardPath);
        }
    }

    public synchronized void addFileReferences(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> dfSegregatedFiles = segregateFilesByFormat(snapshot);
        for (Map.Entry<String, Collection<String>> entry : dfSegregatedFiles.entrySet()) {
            String dataFormat = entry.getKey();
            Map<String, AtomicInteger> dfFileRefCounts = fileRefCounts.computeIfAbsent(dataFormat, k -> new HashMap<>());
            Collection<String> files = entry.getValue();
            for (String file : files) {
                dfFileRefCounts.computeIfAbsent(file, k -> new AtomicInteger(0)).incrementAndGet();
            }
        }
    }

    public synchronized void removeFileReferences(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> dfSegregatedFiles = segregateFilesByFormat(snapshot);
        Map<String, Collection<String>> dfFilesToDelete = new HashMap<>();

        for (Map.Entry<String, Collection<String>> entry : dfSegregatedFiles.entrySet()) {
            String dataFormat = entry.getKey();
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
            dfFilesToDelete.put(dataFormat, filesToDelete);
        }

        if (!dfFilesToDelete.isEmpty()) {
            System.out.println("Files to delete : " + dfFilesToDelete);
            deleteUnreferencedFiles(dfFilesToDelete);
        }
        System.out.println("IndexFileDeleter after removeFileReferences: " + this.toString());
    }

    private Map<String, Collection<String>> segregateFilesByFormat(CatalogSnapshot snapshot) {
        Map<String, Collection<String>> dfSegregatedFiles = new HashMap<>();
        Set<String> dataFormats = snapshot.getDataFormats();
        for (String dataFormat : dataFormats) {
            Collection<String> dfFiles = new HashSet<>();
            Collection<WriterFileSet> fileSets = snapshot.getSearchableFiles(dataFormat);
            for (WriterFileSet fileSet : fileSets) {
                for (String file : fileSet.getFiles()) {
                    dfFiles.add(fileSet.getDirectory() + "/" + file);
                }
            }
            dfSegregatedFiles.put(dataFormat, dfFiles);
        }
        return dfSegregatedFiles;
    }

    private void deleteUnreferencedFiles(ShardPath shardPath) throws IOException {
        if (fileRefCounts.isEmpty())
            return;
        Map<String, Collection<String>> dfFilesToDelete = new HashMap<>();
        for (Map.Entry<String, Map<String, AtomicInteger>> entry : fileRefCounts.entrySet()) {
            String dataFormat = entry.getKey();
            Collection<String> referencedFiles = entry.getValue().keySet();
            Collection<String> filesToDelete = new HashSet<>();
            // TODO - Currently hardcoding to get all parquet files in data path. Fix this
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardPath.getDataPath(), "*.parquet")) {
                StreamSupport.stream(stream.spliterator(), false)
                        .map(Path::toString)
                        .filter((file) -> (!referencedFiles.contains(file)))
                        .forEach(filesToDelete::add);
            }
            filesToDelete = filesToDelete.stream().map(file -> shardPath.getDataPath().resolve(file).toString()).collect(Collectors.toSet());
            dfFilesToDelete.put(dataFormat, filesToDelete);
        }
        deleteUnreferencedFiles(dfFilesToDelete);
    }

    private void deleteUnreferencedFiles(Map<String, Collection<String>> dfFilesToDelete) {
        try {
            engine.deleteFiles(dfFilesToDelete);
        } catch (Exception e) {
            System.err.println("Failed to delete unreferenced files: " + dfFilesToDelete + ", error: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "IndexFileDeleter{fileRefCounts=" + fileRefCounts + "}";
    }
}