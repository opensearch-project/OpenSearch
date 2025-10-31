/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.subdirectory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A store implementation that supports files organized in subdirectories.
 *
 * This store extends the standard OpenSearch Store to handle files that may be
 * located in subdirectories within the shard data path. It provides support
 * for peer recovery operations by ensuring subdirectory files are properly
 * transferred between nodes.
 *
 * The store wraps the underlying Lucene Directory with a {@link SubdirectoryAwareDirectory}
 * that handles path resolution and file operations across subdirectories.
 */
public class SubdirectoryAwareStore extends Store {

    private static final Logger logger = LogManager.getLogger(SubdirectoryAwareStore.class);

    /**
     * Constructor for SubdirectoryAwareStore.
     *
     * @param shardId the shard ID
     * @param indexSettings the index settings
     * @param directory the directory to use for the store
     * @param shardLock the shard lock
     * @param onClose the on close callback
     * @param shardPath the shard path
     */
    public SubdirectoryAwareStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        ShardLock shardLock,
        OnClose onClose,
        ShardPath shardPath
    ) {
        super(shardId, indexSettings, new SubdirectoryAwareDirectory(directory, shardPath), shardLock, onClose, shardPath);
    }

    /**
     * Constructor for SubdirectoryAwareStore.
     *
     * @param shardId the shard ID
     * @param indexSettings the index settings
     * @param directory the directory to use for the store
     * @param shardLock the shard lock
     * @param onClose the on close callback
     * @param shardPath the shard path
     * @param directoryFactory the directory factory
     */
    public SubdirectoryAwareStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        ShardLock shardLock,
        OnClose onClose,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory directoryFactory
    ) {
        super(
            shardId,
            indexSettings,
            new SubdirectoryAwareDirectory(directory, shardPath),
            shardLock,
            onClose,
            shardPath,
            directoryFactory
        );
    }

    @Override
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        long totalNumDocs = 0;

        // Load regular segment files metadata
        final SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(commit);
        MetadataSnapshot.LoadedMetadata regularMetadata = MetadataSnapshot.loadMetadata(segmentCommitInfos, super.directory(), logger);
        Map<String, StoreFileMetadata> builder = new HashMap<>(regularMetadata.fileMetadata);
        Map<String, String> commitUserDataBuilder = new HashMap<>(regularMetadata.userData);
        totalNumDocs += regularMetadata.numDocs;

        // Load subdirectory files metadata from segments_N files in subdirectories
        totalNumDocs += this.loadSubdirectoryMetadataFromSegments(commit, builder);

        return new MetadataSnapshot(Collections.unmodifiableMap(builder), Collections.unmodifiableMap(commitUserDataBuilder), totalNumDocs);
    }

    /**
     * Load subdirectory file metadata by reading segments_N files from any subdirectories.
     * This leverages the same approach as Store.loadMetadata but for files in subdirectories.
     *
     * @return the total number of documents in all subdirectory segments
     */
    private long loadSubdirectoryMetadataFromSegments(IndexCommit commit, Map<String, StoreFileMetadata> builder) throws IOException {
        // Find all segments_N files in subdirectories from the commit
        Set<String> subdirectorySegmentFiles = new HashSet<>();
        for (String fileName : commit.getFileNames()) {
            if (Path.of(fileName).getParent() != null && fileName.contains(IndexFileNames.SEGMENTS)) {
                subdirectorySegmentFiles.add(fileName);
            }
        }

        long totalSubdirectoryNumDocs = 0;
        // Process each subdirectory segments_N file
        for (String segmentsFilePath : subdirectorySegmentFiles) {
            totalSubdirectoryNumDocs += this.loadMetadataFromSubdirectorySegmentsFile(segmentsFilePath, builder);
        }

        return totalSubdirectoryNumDocs;
    }

    /**
     * Load metadata from a specific subdirectory segments_N file
     *
     * @return the number of documents in this segments file
     */
    private long loadMetadataFromSubdirectorySegmentsFile(String segmentsFilePath, Map<String, StoreFileMetadata> builder)
        throws IOException {
        // Parse the directory path from the segments file path
        // e.g., "subdir/path/segments_1" -> "subdir/path"
        Path filePath = Path.of(segmentsFilePath);
        Path parent = filePath.getParent();
        if (parent == null) {
            return 0; // Invalid path - no parent directory
        }

        String segmentsFileName = filePath.getFileName().toString();
        Path subdirectoryFullPath = this.shardPath().getDataPath().resolve(parent.toString());

        try (Directory subdirectory = FSDirectory.open(subdirectoryFullPath)) {
            // Read the SegmentInfos from the segments file
            SegmentInfos segmentInfos = SegmentInfos.readCommit(subdirectory, segmentsFileName);

            // Use the same pattern as Store.loadMetadata to extract file metadata
            loadMetadataFromSegmentInfos(segmentInfos, subdirectory, builder, parent);

            // Return the number of documents in this segments file
            return Lucene.getNumDocs(segmentInfos);
        }
    }

    /**
     * Load metadata from SegmentInfos by reusing Store.MetadataSnapshot.loadMetadata
     */
    private static void loadMetadataFromSegmentInfos(
        SegmentInfos segmentInfos,
        Directory directory,
        Map<String, StoreFileMetadata> builder,
        Path pathPrefix
    ) throws IOException {
        // Reuse the existing Store.loadMetadata method
        Store.MetadataSnapshot.LoadedMetadata loadedMetadata = Store.MetadataSnapshot.loadMetadata(
            segmentInfos,
            directory,
            SubdirectoryAwareStore.logger,
            false
        );

        // Add all files with proper relative path prefix
        for (StoreFileMetadata metadata : loadedMetadata.fileMetadata.values()) {
            String prefixedName = pathPrefix.resolve(metadata.name()).toString();
            StoreFileMetadata prefixedMetadata = new StoreFileMetadata(
                prefixedName,
                metadata.length(),
                metadata.checksum(),
                metadata.writtenBy(),
                metadata.hash()
            );
            builder.put(prefixedName, prefixedMetadata);
        }
    }

    /**
     * A Lucene Directory implementation that handles files in subdirectories.
     *
     * This directory wrapper enables file operations across subdirectories within
     * the shard data path. It resolves paths, creates necessary directory structures,
     * and delegates actual file operations to appropriate filesystem locations.
     */
    public static class SubdirectoryAwareDirectory extends FilterDirectory {
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
}
