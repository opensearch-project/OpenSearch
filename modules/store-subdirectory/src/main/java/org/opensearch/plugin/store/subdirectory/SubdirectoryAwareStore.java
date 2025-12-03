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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
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

    /**
     * Build a MetadataSnapshot that includes file metadata and user data from the commit and from files located in shard subdirectories.
     *
     * Aggregates metadata read from the commit's SegmentInfos and augments it with metadata discovered in subdirectories; the snapshot's document count includes documents found in subdirectory segment files.
     *
     * @param commit the Lucene index commit to read metadata from
     * @return a MetadataSnapshot containing an immutable map of file metadata, an immutable map of commit user data, and the total document count across root and subdirectory files
     * @throws IOException if reading segment information or subdirectory files fails
     */
    @Override
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        long totalNumDocs = 0;

        // Load regular segment files metadata
        final SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(commit);
        MetadataSnapshot.LoadedMetadata regularMetadata = MetadataSnapshot.loadMetadata(segmentCommitInfos, super.directory(), logger);
        Map<String, StoreFileMetadata> builder = new HashMap<>(regularMetadata.fileMetadata);
        Map<String, String> commitUserDataBuilder = new HashMap<>(regularMetadata.userData);
        totalNumDocs += regularMetadata.numDocs;

        // Load subdirectory files metadata (both segment files and non-segment files like custom metadata file)
        totalNumDocs += this.loadSubdirectoryMetadata(commit, builder);

        return new MetadataSnapshot(Collections.unmodifiableMap(builder), Collections.unmodifiableMap(commitUserDataBuilder), totalNumDocs);
    }

    /**
     * Load metadata for files located in shard subdirectories, reading segment files (segments_*)
     * to collect per-segment document counts and computing metadata for other subdirectory files when missing.
     *
     * @param commit the index commit whose file list may include subdirectory paths
     * @param builder a mutable map to populate with discovered StoreFileMetadata keyed by file path
     * @return the total number of documents contained in all discovered subdirectory segment files
     * @throws IOException if reading subdirectory segment or file contents fails
     */
    private long loadSubdirectoryMetadata(IndexCommit commit, Map<String, StoreFileMetadata> builder) throws IOException {
        // Categorize subdirectory files into segment info files (segments_N) and non-segment-info files
        Set<String> subdirectorySegmentInfoFiles = new HashSet<>();
        Set<String> subdirectoryNonSegmentInfoFiles = new HashSet<>();

        for (String fileName : commit.getFileNames()) {
            Path filePath = Path.of(fileName);
            // Only process subdirectory files (files with a parent path)
            if (filePath.getParent() != null) {
                if (fileName.contains(IndexFileNames.SEGMENTS)) {
                    subdirectorySegmentInfoFiles.add(fileName);
                } else {
                    subdirectoryNonSegmentInfoFiles.add(fileName);
                }
            }
        }

        long totalSubdirectoryNumDocs = 0;
        // Process each subdirectory segments_N file
        for (String segmentInfoFilePath : subdirectorySegmentInfoFiles) {
            totalSubdirectoryNumDocs += this.loadMetadataFromSubdirectorySegmentsFile(segmentInfoFilePath, builder);
        }

        // Process non-segment files that weren't loaded by segmentInfo
        for (String nonSegmentInfoFile : subdirectoryNonSegmentInfoFiles) {
            if (!builder.containsKey(nonSegmentInfoFile)) {
                computeFileMetadata(nonSegmentInfoFile, builder);
            }
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
     * Load file metadata from the given SegmentInfos and insert entries into the provided builder
     * with each file name prefixed by the given path prefix.
     *
     * @param segmentInfos the SegmentInfos to read metadata from
     * @param directory the Directory that contains the segment files
     * @param builder a map into which prefixed StoreFileMetadata entries will be inserted; existing
     *                entries with the same prefixed name will be overwritten
     * @param pathPrefix the relative path prefix to prepend to each file name when creating map keys
     * @throws IOException if reading segment metadata from the directory fails
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
     * Computes metadata for a single file under the shard data path and inserts it into the provided builder map.
     *
     * @param fileName the file path relative to the shard data path
     * @param builder  map to receive the computed StoreFileMetadata keyed by the relative file path
     * @throws IOException if reading the file fails
     */
    private void computeFileMetadata(String fileName, Map<String, StoreFileMetadata> builder) throws IOException {
        Path filePath = shardPath().getDataPath().resolve(fileName);
        try (Directory dir = FSDirectory.open(filePath.getParent())) {
            String localFileName = filePath.getFileName().toString();
            try (IndexInput in = dir.openInput(localFileName, IOContext.READONCE)) {
                long length = in.length();
                String checksum = Store.digestToString(CodecUtil.checksumEntireFile(in));
                Version version = org.opensearch.Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion;
                builder.put(fileName, new StoreFileMetadata(fileName, length, checksum, version, null));
            }
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