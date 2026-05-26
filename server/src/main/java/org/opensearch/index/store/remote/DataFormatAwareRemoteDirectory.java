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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteIndexInput;
import org.opensearch.index.store.RemoteIndexOutput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * DataFormatAwareRemoteDirectory extends RemoteDirectory with format-aware blob routing.
 *
 * <p>This directory routes file operations to format-specific BlobContainers. Format resolution
 * depends on the caller:
 * <ul>
 *   <li>Remote blob operations (deleteFile, openInput, fileLength, openBlockInput): receive plain
 *       blob keys (e.g., "_0.pqt__UUID") from RSSD and resolve format via {@code blobFormatCache}</li>
 *   <li>Upload operations (copyFrom): receive local filenames with "format/file" convention
 *       (e.g., "parquet/_0.pqt") and parse format directly via {@link FileMetadata}</li>
 *   <li>FileMetadata-based APIs: receive format explicitly via the FileMetadata object</li>
 * </ul>
 *
 * <p>Blob container routing:
 * <ul>
 *   <li>"lucene" format (or no format) → inherited blobContainer at baseBlobPath (same as RemoteDirectory)</li>
 *   <li>Non-lucene formats (e.g., "parquet") → baseBlobPath/formatName/ sub-path</li>
 * </ul>
 *
 * @opensearch.api
 */
@InternalApi
public class DataFormatAwareRemoteDirectory extends RemoteDirectory {

    private static final String DEFAULT_FORMAT = "lucene";

    private final UnaryOperator<OffsetRangeInputStream> uploadRateLimiter;
    private final UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter;
    private final DownloadRateLimiterProvider downloadRateLimiterProvider;

    private final FormatBlobRouter formatBlobRouter;
    private final Logger logger;

    /**
     * Full constructor with all rate limiter parameters.
     */
    public DataFormatAwareRemoteDirectory(
        BlobStore blobStore,
        BlobPath baseBlobPath,
        UnaryOperator<OffsetRangeInputStream> uploadRateLimiter,
        UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter,
        UnaryOperator<InputStream> downloadRateLimiter,
        UnaryOperator<InputStream> lowPriorityDownloadRateLimiter,
        Map<String, String> pendingDownloadMergedSegments,
        Logger logger,
        DataFormatRegistry dataFormatRegistry,
        IndexSettings indexSettings
    ) {
        super(
            blobStore.blobContainer(baseBlobPath),
            uploadRateLimiter,
            lowPriorityUploadRateLimiter,
            downloadRateLimiter,
            lowPriorityDownloadRateLimiter,
            pendingDownloadMergedSegments
        );
        this.formatBlobRouter = new FormatBlobRouter(blobStore, baseBlobPath);
        this.uploadRateLimiter = uploadRateLimiter;
        this.lowPriorityUploadRateLimiter = lowPriorityUploadRateLimiter;
        this.downloadRateLimiterProvider = new DownloadRateLimiterProvider(downloadRateLimiter, lowPriorityDownloadRateLimiter);
        this.logger = logger;

        // Pre-register format-specific BlobContainers from DataFormatRegistry
        if (dataFormatRegistry != null && indexSettings != null) {
            for (String formatName : dataFormatRegistry.getFormatDescriptors(indexSettings).keySet()) {
                formatBlobRouter.registerFormat(formatName);
            }
        }

        logger.debug("Created DataFormatAwareRemoteDirectory with formats: {}", formatBlobRouter.registeredFormats());
    }

    // ═══════════════════════════════════════════════════════════════
    // Format Routing — delegates to FormatBlobRouter
    // ═══════════════════════════════════════════════════════════════

    /**
     * Resolve the data format for a plain blob key using the format cache.
     * Delegates to {@link FormatBlobRouter#resolveFormat(String)}.
     *
     * @param name the blob key to resolve format for
     * @return the resolved data format name, defaults to "lucene"
     */
    private String resolveFormat(String name) {
        return formatBlobRouter.resolveFormat(name);
    }

    /**
     * Get BlobContainer for a specific data format.
     * Delegates to {@link FormatBlobRouter#containerFor(String)}.
     *
     * @param format the data format name (e.g., "lucene", "parquet")
     * @return BlobContainer for the format
     */
    public BlobContainer getBlobContainerForFormat(String format) {
        return formatBlobRouter.containerFor(format);
    }

    /**
     * Returns the {@link FormatBlobRouter} for direct access by callers that need
     * format-aware blob operations (e.g., listing all blobs across formats).
     *
     * @return the format blob router
     */
    @Override
    public Optional<FormatBlobRouter> getFormatBlobRouter() {
        return Optional.of(formatBlobRouter);
    }

    // ═══════════════════════════════════════════════════════════════
    // Aggregated operations across all format containers
    // ═══════════════════════════════════════════════════════════════

    /**
     * Lists all blobs across the base container and all format-specific containers.
     * Results are sorted in UTF-16 order as required by the Directory contract.
     */
    @Override
    public String[] listAll() throws IOException {
        Map<String, BlobMetadata> allBlobs = formatBlobRouter.listAllBlobs();
        String[] result = allBlobs.keySet().toArray(new String[0]);
        Arrays.sort(result);
        return result;
    }

    /**
     * Format-aware deleteFile override.
     *
     * <p>Uses {@link #resolveFormat(String)} to determine which BlobContainer to delete from.
     * Name is always a plain blob key (e.g., "_0.pqt__UUID") from RSSD, resolved via blobFormatCache.
     * Falls back to "lucene" (base container) if no format info is available.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);
        container.deleteBlobsIgnoringIfNotExists(Collections.singletonList(name));
    }

    /**
     * Format-aware batch delete override.
     *
     * <p>Note: This method receives plain blob names (e.g., "_0.parquet__UUID") without format info,
     * so it attempts deletion from ALL containers (base + format-specific). This is used during
     * stale segment cleanup where the caller doesn't have format information.
     */
    @Override
    public void deleteFiles(List<String> names) throws IOException {
        if (names == null || names.isEmpty()) {
            return;
        }
        // Delete from base container (handles lucene/metadata blobs)
        super.deleteFiles(names);

        // Broadcast delete to every format-specific container. This is intentionally speculative:
        // blob names are UUID-suffixed and globally unique, so at most one container holds each
        // blob.
        for (String format : formatBlobRouter.registeredFormats()) {
            formatBlobRouter.containerFor(format).deleteBlobsIgnoringIfNotExists(names);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // String-based overrides — called by RemoteSegmentStoreDirectory
    // These parse "format/file" from the src string
    // ═══════════════════════════════════════════════════════════════

    /**
     * Sync copyFrom override that properly handles format-aware local files.
     *
     * <p>When AsyncMultiStreamBlobContainer is not available (e.g., FS-based blob store in tests),
     * this fallback is used. The src string may contain "format/" prefix (e.g., "parquet/_0.pqt")
     * which the source DataFormatAwareStoreDirectory handles via parseFilePath().
     */
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        logger.debug("Sync copyFrom: src={}, dest={}", src, dest);
        FileMetadata fileMetadata = new FileMetadata(src);
        BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());
        // Read from local directory (DataFormatAwareStoreDirectory handles "format/file" in src)
        // Write to format-specific BlobContainer (lucene→base, parquet→parquet sub-path, etc.)
        try (IndexInput is = from.openInput(src, context); IndexOutput os = new RemoteIndexOutput(dest, container)) {
            os.copyBytes(is, is.length());
        }
    }

    /**
     * Format-aware async copyFrom override.
     *
     * <p>Parses the src string (e.g., "parquet/_0.pqt") to determine format routing.
     * Opens local file using src as-is (DataFormatAwareStoreDirectory handles format/file parsing).
     * Uploads to the format-specific BlobContainer using remoteFileName as the blob key.
     */
    @Override
    public boolean copyFrom(
        Directory from,
        String src,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) {
        try {
            FileMetadata fileMetadata = new FileMetadata(src);
            BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());

            if (container instanceof AsyncMultiStreamBlobContainer) {
                logger.debug(
                    "Format-aware upload: src={}, format={}, remoteFile={}, container={}",
                    src,
                    fileMetadata.dataFormat(),
                    remoteFileName,
                    container.path()
                );
                uploadBlob(from, src, remoteFileName, container, context, postUploadRunner, listener, lowPriorityUpload, cryptoMetadata);
                return true;
            }

            logger.warn("BlobContainer for format {} does not support async multi-stream upload", fileMetadata.dataFormat());
            return false;
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed format-aware upload: src={}, error={}", src, e.getMessage()), e);
            listener.onFailure(e);
            return true; // Handled (even though failed)
        }
    }

    /**
     * Format-aware fileLength override.
     *
     * <p>Receives a plain blob key (e.g., "_0.pqt__UUID") from RSSD and uses
     * {@link #resolveFormat(String)} to look up the format from blobFormatCache.
     */
    @Override
    public long fileLength(String name) throws IOException {
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);

        if (container == null) {
            throw new NoSuchFileException(String.format(java.util.Locale.ROOT, "No container for format %s, file %s", format, name));
        }

        List<BlobMetadata> metadata = container.listBlobsByPrefixInSortedOrder(name, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
        if (metadata.size() == 1 && metadata.get(0).name().equals(name)) {
            return metadata.get(0).length();
        }
        throw new NoSuchFileException(name);
    }

    /**
     * Create output for a specific format.
     */
    public RemoteIndexOutput createOutput(String remoteFileName, String dataFormat, IOContext context) throws IOException {
        BlobContainer container = getBlobContainerForFormat(dataFormat);
        if (container == null) {
            throw new IOException(String.format(java.util.Locale.ROOT, "No container for format %s, file %s", dataFormat, remoteFileName));
        }
        return new RemoteIndexOutput(remoteFileName, container);
    }

    // ═══════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════

    @Override
    public void delete() throws IOException {
        // Delete all format-specific containers. Each container may be at a separate path
        // (e.g., sibling of base), and may already have been removed by a concurrent cleanup,
        // so tolerate NoSuchFileException.
        for (String format : formatBlobRouter.registeredFormats()) {
            try {
                formatBlobRouter.containerFor(format).delete();
            } catch (java.nio.file.NoSuchFileException ignored) {
                // already deleted — nothing to do
            }
        }
        // Also delete the base container (inherited from RemoteDirectory)
        try {
            super.delete();
        } catch (java.nio.file.NoSuchFileException ignored) {
            // already deleted
        }
        logger.debug("Deleted all containers from DataFormatAwareRemoteDirectory");
    }

    @Override
    public void close() throws IOException {
        formatBlobRouter.clearBlobFormatCache();
    }

    @Override
    public String toString() {
        return "DataFormatAwareRemoteDirectory{"
            + "formats="
            + formatBlobRouter.registeredFormats()
            + ", basePath="
            + formatBlobRouter.basePath()
            + '}';
    }

    // ═══════════════════════════════════════════════════════════════
    // Private upload helpers
    // ═══════════════════════════════════════════════════════════════

    /**
     * Upload blob using String-based src. Opens local file from 'from' directory using src as-is.
     * The target BlobContainer is determined by the caller (format-aware routing already done).
     */
    private void uploadBlob(
        Directory from,
        String src,
        String remoteFileName,
        BlobContainer targetContainer,
        IOContext ioContext,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) throws Exception {
        assert ioContext != IOContext.READONCE : "Remote upload will fail with IoContext.READONCE";
        long expectedChecksum;
        DataFormatAwareStoreDirectory dfasd = DataFormatAwareStoreDirectory.unwrap(from);
        if (dfasd != null) {
            expectedChecksum = dfasd.calculateTransferChecksum(src);
        } else {
            expectedChecksum = calculateChecksumOfChecksum(from, src);
        }
        IndexInput indexInput = from.openInput(src, ioContext);
        try {
            long contentLength = indexInput.length();
            boolean remoteIntegrityEnabled = (targetContainer instanceof AsyncMultiStreamBlobContainer)
                && ((AsyncMultiStreamBlobContainer) targetContainer).remoteIntegrityCheckSupported();

            lowPriorityUpload = lowPriorityUpload || contentLength > ByteSizeUnit.GB.toBytes(15);

            RemoteTransferContainer.OffsetRangeInputStreamSupplier supplier = lowPriorityUpload
                ? (size, position) -> lowPriorityUploadRateLimiter.apply(
                    new OffsetRangeIndexInputStream(indexInput.clone(), size, position)
                )
                : (size, position) -> uploadRateLimiter.apply(new OffsetRangeIndexInputStream(indexInput.clone(), size, position));

            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                src,
                remoteFileName,
                contentLength,
                true,
                lowPriorityUpload ? WritePriority.LOW : WritePriority.NORMAL,
                supplier,
                expectedChecksum,
                remoteIntegrityEnabled
            );

            ActionListener<Void> completionListener = createCompletionListener(
                src,
                postUploadRunner,
                listener,
                remoteTransferContainer,
                indexInput
            );

            WriteContext writeContext = remoteTransferContainer.createWriteContext();
            ((AsyncMultiStreamBlobContainer) targetContainer).asyncBlobUpload(writeContext, completionListener);
        } catch (Exception e) {
            logger.warn("Exception while calling asyncBlobUpload for {}, closing IndexInput", src);
            indexInput.close();
            throw e;
        }
    }

    /**
     * Opens a stream for reading the existing file and returns {@link RemoteIndexInput} enclosing
     * the stream.
     *
     * <p>Receives a plain blob key (e.g., "_0.pqt__UUID") from RSSD and uses
     * {@link #resolveFormat(String)} to look up the format from blobFormatCache.
     *
     * @param name the name of an existing file.
     * @param fileLength file length
     * @param context desired {@link IOContext} context
     * @return the {@link RemoteIndexInput} enclosing the stream
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public IndexInput openInput(String name, long fileLength, IOContext context) throws IOException {
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);
        InputStream inputStream = null;
        try {
            inputStream = container.readBlob(name);
            UnaryOperator<InputStream> rateLimiter = downloadRateLimiterProvider.get(name);
            return new RemoteIndexInput(name, rateLimiter.apply(inputStream), fileLength);
        } catch (Exception e) {
            // In case the RemoteIndexInput creation fails, close the input stream to avoid file handler leak.
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            logger.error("Exception while reading blob for file: {} format: {} path: {}", name, format, blobContainer.path());
            throw e;
        }
    }

    /**
     * Format-aware openBlockInput override.
     *
     * <p>Receives a plain blob key (e.g., "_0.pqt__UUID") from RSSD and uses
     * {@link #resolveFormat(String)} to look up the format from blobFormatCache.
     *
     * @param name the name of an existing file (blob key).
     * @param position block start position
     * @param length block length
     * @param fileLength total file length
     * @param context desired {@link IOContext} context
     * @return the {@link IndexInput} enclosing the block data
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public IndexInput openBlockInput(String name, long position, long length, long fileLength, IOContext context) throws IOException {
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);
        if (position < 0 || length <= 0 || (position + length > fileLength)) {
            throw new IllegalArgumentException("Invalid values of block start and size");
        }
        byte[] bytes;
        try (InputStream inputStream = container.readBlob(name, position, length)) {
            UnaryOperator<InputStream> rateLimiter = downloadRateLimiterProvider.get(name);
            bytes = rateLimiter.apply(inputStream).readAllBytes();
        } catch (Exception e) {
            logger.error("Exception while reading block for file: {} format: {} path: {}", name, format, blobContainer.path());
            throw e;
        }
        return new ByteArrayIndexInput(name, bytes);
    }

    private ActionListener<Void> createCompletionListener(
        String fileName,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        RemoteTransferContainer remoteTransferContainer,
        IndexInput indexInput
    ) {
        ActionListener<Void> completionListener = ActionListener.wrap(resp -> {
            try {
                postUploadRunner.run();
                listener.onResponse(null);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Exception in segment postUpload for file [{}]", fileName), e);
                listener.onFailure(e);
            }
        }, ex -> {
            logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileName), ex);
            IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(ex);
            if (corruptIndexException != null) {
                listener.onFailure(corruptIndexException);
                return;
            }
            Throwable throwable = ExceptionsHelper.unwrap(ex, CorruptFileException.class);
            if (throwable != null) {
                CorruptFileException cfe = (CorruptFileException) throwable;
                listener.onFailure(new CorruptIndexException(cfe.getMessage(), cfe.getFileName()));
                return;
            }
            listener.onFailure(ex);
        });

        completionListener = ActionListener.runBefore(completionListener, () -> {
            try {
                remoteTransferContainer.close();
            } catch (Exception e) {
                logger.warn("Error closing RemoteTransferContainer", e);
            }
        });

        completionListener = ActionListener.runAfter(completionListener, () -> {
            try {
                indexInput.close();
            } catch (IOException e) {
                logger.warn("Error closing IndexInput", e);
            }
        });

        return completionListener;
    }

    // ═══════════════════════════════════════════════════════════════
    // Private helpers
    // ═══════════════════════════════════════════════════════════════

    private boolean isMergedSegment(String remoteFilename) {
        return pendingDownloadMergedSegments != null && pendingDownloadMergedSegments.containsValue(remoteFilename);
    }

    /**
     * DownloadRateLimiterProvider returns a low-priority rate limited stream if the segment
     * being downloaded is a merged segment.
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
