/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.index.engine.MergedSegmentWarmer;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteIndexInput;
import org.opensearch.index.store.RemoteIndexOutput;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactory;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

import static org.opensearch.index.shard.ShardPath.METADATA_FOLDER_NAME;

/**
 * CompositeRemoteDirectory with direct BlobContainer access per format.
 *
 * Key Architecture:
 * - Map of DataFormat to BlobContainer for direct blob access
 * - Lazy BlobContainer creation when new formats are encountered
 * - Format determination from FileMetadata instead of file extension parsing
 * - ALL formats get equal treatment with same generic streaming logic
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class CompositeRemoteDirectory implements Closeable {

    /**
     * Metadata stream wrapper for reading/writing RemoteSegmentMetadata
     */
    private static final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new RemoteSegmentMetadataHandlerFactory(),
        RemoteSegmentMetadata.VERSION_ONE,
        RemoteSegmentMetadata.CURRENT_VERSION,
        RemoteSegmentMetadata.METADATA_CODEC
    );

    private final UnaryOperator<OffsetRangeInputStream> uploadRateLimiter;
    private final UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter;
    private final DownloadRateLimiterProvider downloadRateLimiterProvider;

    /**
     * Map containing the mapping of segment files that are pending download as part of the pre-copy (warm) phase of
     * {@link MergedSegmentWarmer}. The key is the local filename and value is the remote filename.
     */
    final Map<FileMetadata, String> pendingDownloadMergedSegments;

    private final Map<String, BlobContainer> formatBlobContainers;
    private  final BlobContainer metadataBlobContainer;
    private final BlobStore blobStore;
    private final BlobPath baseBlobPath;
    private final Logger logger;

    /**
     * Full constructor with all rate limiter parameters
     */
    public CompositeRemoteDirectory(
        BlobStore blobStore,
        BlobPath baseBlobPath,
        UnaryOperator<OffsetRangeInputStream> uploadRateLimiter,
        UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter,
        UnaryOperator<InputStream> downloadRateLimiter,
        UnaryOperator<InputStream> lowPriorityDownloadRateLimiter,
        Map<FileMetadata, String> pendingDownloadMergedSegments,
        Logger logger,
        PluginsService pluginsService
    ) {
        this.formatBlobContainers = new ConcurrentHashMap<>();
        this.blobStore = blobStore;
        this.baseBlobPath = baseBlobPath;
        this.uploadRateLimiter = uploadRateLimiter;
        this.lowPriorityUploadRateLimiter = lowPriorityUploadRateLimiter;
        this.downloadRateLimiterProvider = new DownloadRateLimiterProvider(downloadRateLimiter, lowPriorityDownloadRateLimiter);
        this.pendingDownloadMergedSegments = pendingDownloadMergedSegments;
        this.logger = logger;

        BlobPath metadataBlobPath = baseBlobPath.parent().add(METADATA_FOLDER_NAME);
        this.metadataBlobContainer = blobStore.blobContainer(metadataBlobPath);

        try {
            pluginsService.filterPlugins(DataSourcePlugin.class).forEach(
                plugin -> {
                    try {
                        formatBlobContainers.put(plugin.getDataFormat().name(), plugin.createBlobContainer(blobStore, baseBlobPath));
                    } catch (IOException e) {
                        logger.error("failed to create blob container for dataformat {} at base path {}", plugin.getDataFormat().name(), baseBlobPath, e);
                        throw new RuntimeException(e);
                    }
                }
            );
        } catch (NullPointerException e) {
            formatBlobContainers.put("", null);
        }

        logger.debug("Created CompositeRemoteDirectory with {} format BlobContainers",
            formatBlobContainers.size());
    }

    /**
     * Get or create BlobContainer for specific format.
     * This is where the lazy creation happens - if we don't have a BlobContainer
     * for this format yet, we create one and store it in the map.
     *
     * @param format the data format name
     * @return BlobContainer for the format (created if not exists)
     */
    public BlobContainer getBlobContainerForFormat(String format) {
        return formatBlobContainers.computeIfAbsent(format, f -> {
            // Create format-specific BlobPath: basePath/formatName/
            BlobPath formatPath = baseBlobPath.add(f.toLowerCase());
            BlobContainer container = blobStore.blobContainer(formatPath);

            logger.debug("Created new BlobContainer for format {} at path: {}", f, formatPath);
            return container;
        });
    }

    public void copyFrom(CompositeStoreDirectory from, FileMetadata src, String dest, IOContext context)
        throws IOException {
        boolean success = false;
        try (IndexInput is = from.openInput(src, IOContext.READONCE);
             IndexOutput os = createOutput(dest, src.dataFormat(), context)) {
            os.copyBytes(is, is.length());
            success = true;
        } finally {
            if (!success) {
                from.deleteFile(src);
            }
        }
    }

    /**
     * Primary copyFrom method using FileMetadata - similar to RemoteDirectory.copyFrom but with format-aware routing
     */
    public boolean copyFrom(
        CompositeStoreDirectory from,
        FileMetadata fileMetadata,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) {
        try {
            String fileName = fileMetadata.file();
            BlobContainer blobContainer = getBlobContainerForFormat(fileMetadata.dataFormat());

            if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
                logger.debug("Starting format-aware upload: file={}, format={}, container={}",
                           fileName, fileMetadata.dataFormat(), blobContainer.path());
                uploadBlob(from, fileMetadata, remoteFileName, context, postUploadRunner, listener, lowPriorityUpload);
                return true;
            }

            logger.warn("BlobContainer does not support async multi-stream upload: {}", blobContainer.getClass());
            return false;

        } catch (Exception e) {
            logger.error("Failed to start format-aware upload: file={}, error={}", fileMetadata.dataFormat(), e.getMessage(), e);
            listener.onFailure(e);
            return true;  // Return true to indicate we handled it (even though it failed)
        }
    }

    private void uploadBlob(
        CompositeStoreDirectory from,
        FileMetadata src,
        String remoteFileName,
        IOContext ioContext,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) throws Exception {
        assert ioContext != IOContext.READONCE : "Remote upload will fail with IoContext.READONCE";
        String dataFormat = src.dataFormat();
        long expectedChecksum = calculateChecksumOfChecksum(from, src);
        long contentLength;
        IndexInput indexInput = from.openInput(src, ioContext);
        try {
            contentLength = indexInput.length();
            boolean remoteIntegrityEnabled = false;
            if (getBlobContainer(dataFormat) instanceof AsyncMultiStreamBlobContainer) {
                remoteIntegrityEnabled = ((AsyncMultiStreamBlobContainer) getBlobContainer(dataFormat)).remoteIntegrityCheckSupported();
            }
            lowPriorityUpload = lowPriorityUpload || contentLength > ByteSizeUnit.GB.toBytes(15);
            RemoteTransferContainer.OffsetRangeInputStreamSupplier offsetRangeInputStreamSupplier;

            if (lowPriorityUpload) {
                offsetRangeInputStreamSupplier = (size, position) -> lowPriorityUploadRateLimiter.apply(
                    new OffsetRangeIndexInputStream(indexInput.clone(), size, position)
                );
            } else {
                offsetRangeInputStreamSupplier = (size, position) -> uploadRateLimiter.apply(
                    new OffsetRangeIndexInputStream(indexInput.clone(), size, position)
                );
            }
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                src.file(),
                remoteFileName,
                contentLength,
                true,
                lowPriorityUpload ? WritePriority.LOW : WritePriority.NORMAL,
                offsetRangeInputStreamSupplier,
                expectedChecksum,
                remoteIntegrityEnabled,
                null
            );
            ActionListener<Void> completionListener = ActionListener.wrap(resp -> {
                try {
                    postUploadRunner.run();
                    listener.onResponse(null);
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage("Exception in segment postUpload for file [{}]", src), e);
                    listener.onFailure(e);
                }
            }, ex -> {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", src), ex);
                IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(ex);
                if (corruptIndexException != null) {
                    listener.onFailure(corruptIndexException);
                    return;
                }
                Throwable throwable = ExceptionsHelper.unwrap(ex, CorruptFileException.class);
                if (throwable != null) {
                    CorruptFileException corruptFileException = (CorruptFileException) throwable;
                    listener.onFailure(new CorruptIndexException(corruptFileException.getMessage(), corruptFileException.getFileName()));
                    return;
                }
                listener.onFailure(ex);
            });

            completionListener = ActionListener.runBefore(completionListener, () -> {
                try {
                    remoteTransferContainer.close();
                } catch (Exception e) {
                    logger.warn("Error occurred while closing streams", e);
                }
            });

            completionListener = ActionListener.runAfter(completionListener, () -> {
                try {
                    indexInput.close();
                } catch (IOException e) {
                    logger.warn("Error occurred while closing index input", e);
                }
            });

            WriteContext writeContext = remoteTransferContainer.createWriteContext();
            ((AsyncMultiStreamBlobContainer) getBlobContainer(dataFormat)).asyncBlobUpload(writeContext, completionListener);
        } catch (Exception e) {
            logger.warn("Exception while calling asyncBlobUpload, closing IndexInput to avoid leak");
            indexInput.close();
            throw e;
        }
    }

    private BlobContainer getBlobContainer(String df) {
        return formatBlobContainers.get(df);
    }

    private long calculateChecksumOfChecksum(CompositeStoreDirectory from, FileMetadata fileMetadata) throws IOException {
        return from.calculateChecksum(fileMetadata);
    }

    /**

     /**
     * Get file length from the appropriate format-specific BlobContainer
     * @param fileMetadata The File Metadata
     * @return The length of the file
     * @throws IOException If the file is not found or on other IO errors
     */
    public long fileLength(FileMetadata fileMetadata) throws IOException {
        BlobContainer blobContainer = getBlobContainer(fileMetadata.dataFormat());

        if (blobContainer == null) {
            throw new NoSuchFileException(
                String.format("File %s not found in any containers for format %s", fileMetadata.file(), fileMetadata.dataFormat())
            );
        }

        try {
            List<BlobMetadata> metadata = blobContainer.listBlobsByPrefixInSortedOrder(
                fileMetadata.file(), 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);

            if (metadata.size() == 1 && metadata.get(0).name().equals(fileMetadata.file())) {
                return metadata.get(0).length();
            }

            throw new NoSuchFileException(
                String.format("File %s not found in container for format %s", fileMetadata.file(), fileMetadata.dataFormat())
            );
        } catch (IOException e) {
            throw new IOException(
                String.format("Error getting length for file %s in format %s", fileMetadata.file(), fileMetadata.dataFormat()), e
            );
        }
    }

    /**
     * Create output for writing to the appropriate format-specific BlobContainer
     */
    public RemoteIndexOutput createOutput(String remoteFileName, String df, IOContext context) throws IOException {
        if (remoteFileName == null || remoteFileName.isEmpty()) {
            throw new IllegalArgumentException("Remote file name cannot be null or empty");
        }
        try {
            BlobContainer blobContainer = formatBlobContainers.get(df);

            if (blobContainer!=null) {
                logger.debug("File {} already exists, using existing container", remoteFileName);
                return new RemoteIndexOutput(remoteFileName, blobContainer);
            }
            else if(df !=null && df.equals(METADATA_FOLDER_NAME)) {
                return new RemoteIndexOutput(remoteFileName, metadataBlobContainer);
            }

            throw new IOException(
                String.format("Failed to create output for file %s in format %s", remoteFileName, df)
            );
        } catch (Exception e) {
            throw new IOException(
                String.format("Failed to create output for file %s in format %s", remoteFileName, df),
                e
            );
        }
    }

    /**
     * Open input for reading from the appropriate format-specific BlobContainer
     */
    public IndexInput openInput(FileMetadata fileMetadata, long fileLength, IOContext context) throws IOException {
        if (fileMetadata.file() == null || fileMetadata.file().isEmpty()) {
            throw new IllegalArgumentException("Remote file name cannot be null or empty");
        }

        InputStream inputStream = null;
        try {
            BlobContainer blobContainer = getBlobContainer(fileMetadata.dataFormat());
            if (blobContainer==null) {
                throw new IOException(String.format("Failed to find blobContainer for file %s in format %s", fileMetadata.file(), fileMetadata.dataFormat()));
            }

            inputStream = blobContainer.readBlob(fileMetadata.file());
            UnaryOperator<InputStream> rateLimiter = downloadRateLimiterProvider.get(fileMetadata.file());
            return new RemoteIndexInput(fileMetadata.file(), rateLimiter.apply(inputStream), fileLength);
        } catch (Exception e) {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            logger.error("Exception while reading blob for file: " + fileMetadata.file(), e);
            throw e;
        }
    }

    /**
     * Delete the entire CompositeRemoteDirectory
     */
    public void delete() throws IOException {
        for (BlobContainer container : formatBlobContainers.values()) {
            container.delete();
        }
        logger.debug("Deleted all format containers from CompositeRemoteDirectory");
    }


    /**
     * Read the latest metadata file from the metadata blob container.
     * This method provides compatibility with RemoteSegmentStoreDirectory.readLatestMetadataFile()
     */
    public RemoteSegmentMetadata readLatestMetadataFile() throws IOException {
        try {
            logger.info("[SEGMENT_UPLOAD_DEBUG] CompositeRemoteDirectory.readLatestMetadataFile() called, searching with prefix={}", METADATA_FOLDER_NAME);
            List<BlobMetadata> metadataFiles = metadataBlobContainer.listBlobsByPrefixInSortedOrder(
                METADATA_FOLDER_NAME, 10, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);

            logger.info("[SEGMENT_UPLOAD_DEBUG] Found {} metadata files with prefix={}", metadataFiles.size(), METADATA_FOLDER_NAME);
            if (metadataFiles.isEmpty()) {
                logger.info("[SEGMENT_UPLOAD_DEBUG] No metadata files found in composite remote directory");
                return null;
            }

            // Get the latest (first in reverse lexicographic order)
            String latestMetadataFile = metadataFiles.get(0).name();
            logger.info("[SEGMENT_UPLOAD_DEBUG] Reading latest metadata file: {}", latestMetadataFile);
            RemoteSegmentMetadata result = readMetadataFile(latestMetadataFile);
            logger.info("[SEGMENT_UPLOAD_DEBUG] Read metadata file successfully, contains {} segments", 
                       result != null ? result.getMetadata().size() : 0);
            return result;
        } catch (Exception e) {
            logger.error("[SEGMENT_UPLOAD_DEBUG] Failed to read latest metadata file from composite directory", e);
            throw new IOException("Failed to read latest metadata file", e);
        }
    }

    /**
     * Read a specific metadata file by name from the metadata blob container.
     * This method provides compatibility with RemoteSegmentStoreDirectory.readMetadataFile()
     */
    public RemoteSegmentMetadata readMetadataFile(String metadataFileName) throws IOException {
        try (InputStream inputStream = metadataBlobContainer.readBlob(metadataFileName)) {
            byte[] metadataBytes = inputStream.readAllBytes();

            // Use our own metadata stream wrapper
            return metadataStreamWrapper.readStream(
                new ByteArrayIndexInput(metadataFileName, metadataBytes)
            );
        } catch (NoSuchFileException e) {
            logger.debug("Metadata file not found: {}", metadataFileName);
            return null;
        } catch (Exception e) {
            logger.error("Failed to read metadata file: {}", metadataFileName, e);
            throw new IOException("Failed to read metadata file: " + metadataFileName, e);
        }
    }

    @Override
    public void close() throws IOException {
        formatBlobContainers.clear();
    }

    @Override
    public String toString() {
        return "CompositeRemoteDirectory{" +
               "formats=" + formatBlobContainers.keySet() +
               ", basePath=" + baseBlobPath +
               '}';
    }

    private boolean isMergedSegment(String remoteFilename) {
        return pendingDownloadMergedSegments != null && pendingDownloadMergedSegments.containsValue(remoteFilename);
    }

    /**
     * DownloadRateLimiterProvider returns a low-priority rate limited stream if the segment
     * being downloaded is a merged segment as part of the pre-copy (warm) phase of
     * {@link MergedSegmentWarmer}.
     */
    private class DownloadRateLimiterProvider {
        private final UnaryOperator<InputStream> downloadRateLimiter;
        private final UnaryOperator<InputStream> lowPriorityDownloadRateLimiter;

        DownloadRateLimiterProvider(
            UnaryOperator<InputStream> downloadRateLimiter,
            UnaryOperator<InputStream> lowPriorityDownloadRateLimiter
        ) {
            this.downloadRateLimiter = downloadRateLimiter;
            this.lowPriorityDownloadRateLimiter = lowPriorityDownloadRateLimiter;
        }

        public UnaryOperator<InputStream> get(final String filename) {
            if (isMergedSegment(filename)) {
                return lowPriorityDownloadRateLimiter;
            }
            return downloadRateLimiter;
        }
    }
}
