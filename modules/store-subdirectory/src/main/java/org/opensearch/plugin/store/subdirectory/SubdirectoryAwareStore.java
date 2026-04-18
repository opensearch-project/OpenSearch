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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.NativeStoreFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
     * Constructor for SubdirectoryAwareStore with native store support.
     *
     * @param shardId the shard ID
     * @param indexSettings the index settings
     * @param directory the directory to use for the store
     * @param shardLock the shard lock
     * @param onClose the on close callback
     * @param shardPath the shard path
     * @param directoryFactory the directory factory
     * @param nativeStoreFactory factory for creating shard-scoped native object store handles
     */
    public SubdirectoryAwareStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        ShardLock shardLock,
        OnClose onClose,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory directoryFactory,
        NativeStoreFactory nativeStoreFactory
    ) {
        super(
            shardId,
            indexSettings,
            new SubdirectoryAwareDirectory(directory, shardPath),
            shardLock,
            onClose,
            shardPath,
            directoryFactory,
            nativeStoreFactory
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

        // Load subdirectory files metadata (both segment files and non-segment files like custom metadata file)
        totalNumDocs += this.loadSubdirectoryMetadata(commit, builder);

        return new MetadataSnapshot(Collections.unmodifiableMap(builder), Collections.unmodifiableMap(commitUserDataBuilder), totalNumDocs);
    }

    /**
     * Load subdirectory file metadata by reading segments_N files from any subdirectories,
     * and also compute metadata for non-segment files.
     *
     * @return the total number of documents in all subdirectory segments
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
     * Compute and store metadata for a single file.
     *
     * @param fileName the relative file path
     * @param builder the map to add metadata to
     * @throws IOException if reading file fails
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
     * Extends the server's SubdirectoryAwareDirectory for backward compatibility.
     */
    public static class SubdirectoryAwareDirectory extends org.opensearch.index.store.SubdirectoryAwareDirectory {
        /**
         * Creates a new SubdirectoryAwareDirectory wrapping the given delegate.
         *
         * @param delegate  the underlying Lucene directory
         * @param shardPath the shard path for resolving subdirectories
         */
        public SubdirectoryAwareDirectory(Directory delegate, ShardPath shardPath) {
            super(delegate, shardPath);
        }
    }
}
