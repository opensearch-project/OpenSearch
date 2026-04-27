/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Lucene Directory implementation that handles files in subdirectories.
 *
 * This directory wrapper enables file operations across subdirectories within
 * the shard data path. It resolves paths, creates necessary directory structures,
 * and delegates actual file operations to appropriate filesystem locations.
 */
public class SubdirectoryAwareDirectory extends FilterDirectory {
    private static final Logger logger = LogManager.getLogger(SubdirectoryAwareDirectory.class);
    private static final Set<String> EXCLUDED_SUBDIRECTORIES = Set.of("index/", "translog/", "_state/");
    private final ShardPath shardPath;

    /**
     * Constructor for SubdirectoryAwareDirectory.
     *
     * @param delegate the delegate directory
     * @param shardPath the shard path
     */
    public SubdirectoryAwareDirectory(Directory delegate, ShardPath shardPath) {
        super(delegate);
        this.shardPath = shardPath;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return super.openInput(parseFilePath(name), context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        String targetFilePath = parseFilePath(name);
        Path targetFile = Path.of(targetFilePath);
        Files.createDirectories(targetFile.getParent());
        return super.createOutput(targetFilePath, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(parseFilePath(name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        return super.fileLength(parseFilePath(name));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        super.sync(names.stream().map(this::parseFilePath).collect(Collectors.toList()));
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        super.rename(parseFilePath(source), parseFilePath(dest));
    }

    @Override
    public String[] listAll() throws IOException {
        // Get files from the delegate (regular index files)
        String[] delegateFiles = super.listAll();

        // Get subdirectory files by scanning all subdirectories
        Set<String> allFiles = new HashSet<>(Arrays.asList(delegateFiles));
        addSubdirectoryFiles(allFiles);

        return allFiles.stream().sorted().toArray(String[]::new);
    }

    private void addSubdirectoryFiles(Set<String> allFiles) throws IOException {
        Path dataPath = shardPath.getDataPath();
        Files.walkFileTree(dataPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (attrs.isRegularFile()) {
                    Path relativePath = dataPath.relativize(file);
                    // Only add files that are in subdirectories (have a parent directory)
                    if (relativePath.getParent() != null) {
                        String relativePathStr = relativePath.toString();
                        // Exclude index dir (handled in super.listAll()), translog dir, and _state dir
                        if (EXCLUDED_SUBDIRECTORIES.stream().noneMatch(relativePathStr::startsWith)) {
                            allFiles.add(relativePathStr);
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
                if (e instanceof NoSuchFileException) {
                    logger.debug("Skipping inaccessible file during size estimation: {}", file);
                    return FileVisitResult.CONTINUE;
                }
                throw e;
            }
        });
    }

    private String parseFilePath(String fileName) {
        if (Path.of(fileName).getParent() != null) {
            // File path (e.g., "subdirectory/segments_1" or "subdirectory/recovery.xxx.segments_1")
            return shardPath.getDataPath().resolve(fileName).toString();
        } else {
            // Simple filename (e.g., "segments_1") - resolve relative to the shard's index directory
            return shardPath.resolveIndex().resolve(fileName).toString();
        }
    }
}
