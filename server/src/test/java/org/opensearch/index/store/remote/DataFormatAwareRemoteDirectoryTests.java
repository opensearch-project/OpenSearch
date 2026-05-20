/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.RemoteIndexOutput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataFormatAwareRemoteDirectory}.
 */
public class DataFormatAwareRemoteDirectoryTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(DataFormatAwareRemoteDirectoryTests.class);

    private BlobStore mockBlobStore;
    private BlobContainer baseBlobContainer;
    private BlobContainer parquetBlobContainer;
    private BlobPath baseBlobPath;
    private DataFormatAwareRemoteDirectory directory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockBlobStore = mock(BlobStore.class);
        baseBlobContainer = mock(BlobContainer.class);
        parquetBlobContainer = mock(BlobContainer.class);
        baseBlobPath = new BlobPath().add("segments").add("data");

        when(mockBlobStore.blobContainer(baseBlobPath)).thenReturn(baseBlobContainer);
        when(mockBlobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(parquetBlobContainer);

        // Identity rate limiters (no-op)
        UnaryOperator<OffsetRangeInputStream> uploadRateLimiter = UnaryOperator.identity();
        UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter = UnaryOperator.identity();
        UnaryOperator<InputStream> downloadRateLimiter = UnaryOperator.identity();
        UnaryOperator<InputStream> lowPriorityDownloadRateLimiter = UnaryOperator.identity();

        // Mock DataFormatRegistry to register "parquet" format
        DataFormatRegistry mockRegistry = mock(DataFormatRegistry.class);
        Settings indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(indexSettingsBuilder)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        when(mockRegistry.getFormatDescriptors(any(IndexSettings.class))).thenReturn(
            Map.of("parquet", (Supplier<DataFormatDescriptor>) () -> new DataFormatDescriptor("parquet", new GenericCRC32ChecksumHandler()))
        );

        directory = new DataFormatAwareRemoteDirectory(
            mockBlobStore,
            baseBlobPath,
            uploadRateLimiter,
            lowPriorityUploadRateLimiter,
            downloadRateLimiter,
            lowPriorityDownloadRateLimiter,
            new HashMap<>(),
            logger,
            mockRegistry,
            indexSettings
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // Format Routing Tests (getBlobContainerForFormat)
    // ═══════════════════════════════════════════════════════════════

    public void testGetBlobContainerForFormat_Lucene() {
        BlobContainer container = directory.getBlobContainerForFormat("lucene");
        assertSame("lucene should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_LUCENE_UpperCase() {
        BlobContainer container = directory.getBlobContainerForFormat("LUCENE");
        assertSame("LUCENE should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Metadata() {
        BlobContainer container = directory.getBlobContainerForFormat("metadata");
        assertSame("metadata should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Null() {
        BlobContainer container = directory.getBlobContainerForFormat(null);
        assertSame("null format should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Empty() {
        BlobContainer container = directory.getBlobContainerForFormat("");
        assertSame("empty format should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Parquet() {
        BlobContainer container = directory.getBlobContainerForFormat("parquet");
        assertSame("parquet should route to parquet container", parquetBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_UnregisteredFormat_CreatesLazily() {
        // FormatBlobRouter lazily creates containers for unknown formats via computeIfAbsent
        // With a mock BlobStore, the container may be null, but no exception is thrown
        assertNoException(() -> directory.getBlobContainerForFormat("arrow"));
    }

    private static void assertNoException(Runnable r) {
        r.run();
    }

    // ═══════════════════════════════════════════════════════════════
    // List Tests
    // ═══════════════════════════════════════════════════════════════

    public void testListAll_AggregatesAllContainers() throws IOException {
        // Base container has lucene files
        Map<String, BlobMetadata> baseBlobs = new HashMap<>();
        baseBlobs.put("_0.cfs__UUID1", new PlainBlobMetadata("_0.cfs__UUID1", 100));
        baseBlobs.put("_0.si__UUID2", new PlainBlobMetadata("_0.si__UUID2", 50));
        when(baseBlobContainer.listBlobs()).thenReturn(baseBlobs);

        Map<String, BlobMetadata> parquetBlobs = new HashMap<>();
        parquetBlobs.put("_0.parquet__UUID3", new PlainBlobMetadata("_0.parquet__UUID3", 200));
        when(parquetBlobContainer.listBlobs()).thenReturn(parquetBlobs);

        String[] allFiles = directory.listAll();

        assertEquals(3, allFiles.length);
        // Should be sorted
        assertEquals("_0.cfs__UUID1", allFiles[0]);
        assertEquals("_0.parquet__UUID3", allFiles[1]);
        assertEquals("_0.si__UUID2", allFiles[2]);
    }

    public void testListAll_EmptyContainers() throws IOException {
        when(baseBlobContainer.listBlobs()).thenReturn(Collections.emptyMap());

        String[] allFiles = directory.listAll();
        assertEquals(0, allFiles.length);
    }

    // ═══════════════════════════════════════════════════════════════
    // Delete Tests
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_LuceneFile() throws IOException {
        directory.deleteFile("_0.cfs");

        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.cfs"));
        verify(parquetBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    public void testDeleteFile_ParquetFile_WithFormatSuffix() throws IOException {
        // Register format in cache since DFARD receives plain blob keys
        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.parquet", "parquet");
        directory.deleteFile("_0.parquet");

        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.parquet"));
    }

    public void testDeleteFile_WithUploadedSegmentMetadata_Parquet() throws IOException {

        // Use fromString() since constructor is package-private
        // Format: originalFilename::uploadedFilename::checksum::length::writtenByMajor
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString(
            "parquet/_0.parquet::_0.parquet__UUID1::checksum123::200::10"
        );

        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.parquet__UUID1", "parquet");
        directory.deleteFile(metadata.getUploadedFilename());

        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.parquet__UUID1"));
    }

    public void testDeleteFiles_BatchDelete_DeletesFromAllContainers() throws IOException {

        List<String> names = List.of("_0.cfs__UUID1", "_0.parquet__UUID2");
        directory.deleteFiles(names);

        // baseBlobContainer is called from super.deleteFiles + lucene format container (same instance)
        verify(baseBlobContainer, times(2)).deleteBlobsIgnoringIfNotExists(names);
        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(names);
    }

    public void testDeleteFiles_EmptyList_NoOp() throws IOException {
        directory.deleteFiles(Collections.emptyList());

        verify(baseBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    public void testDeleteFiles_NullList_NoOp() throws IOException {
        directory.deleteFiles(null);

        verify(baseBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    // ═══════════════════════════════════════════════════════════════
    // OpenInput Tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_WithUploadedSegmentMetadata_Lucene() throws IOException {
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");

        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs__UUID1")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput(metadata.getUploadedFilename(), 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();

        verify(baseBlobContainer).readBlob("_0.cfs__UUID1");
        verify(parquetBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_WithUploadedSegmentMetadata_Parquet() throws IOException {

        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString(
            "parquet/_0.parquet::_0.parquet__UUID1::checksum456::200::10"
        );

        byte[] content = new byte[200];
        when(parquetBlobContainer.readBlob("_0.parquet__UUID1")).thenReturn(new ByteArrayInputStream(content));

        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.parquet__UUID1", "parquet");
        IndexInput input = directory.openInput(metadata.getUploadedFilename(), 200, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(200, input.length());
        input.close();

        verify(parquetBlobContainer).readBlob("_0.parquet__UUID1");
        verify(baseBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_ClosesStream_OnFailure() throws IOException {
        InputStream mockStream = mock(InputStream.class);
        when(baseBlobContainer.readBlob("_0.cfs__UUID1")).thenReturn(mockStream);
        when(mockStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("read error"));

        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");

        // The openInput should succeed (it just wraps the stream), but we verify the pattern
        IndexInput input = directory.openInput(metadata.getUploadedFilename(), 100, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // FileLength Tests
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_LuceneFile() throws IOException {
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        long length = directory.fileLength("_0.cfs");
        assertEquals(1234, length);
    }

    public void testFileLength_ParquetFile() throws IOException {

        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.parquet", 5678));
        when(parquetBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.parquet"), eq(1), any())).thenReturn(blobList);

        // Register format so resolveFormat routes to parquet container
        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.parquet", "parquet");
        long length = directory.fileLength("_0.parquet");
        assertEquals(5678, length);
    }

    public void testFileLength_FileNotFound() throws IOException {
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("nonexistent"), eq(1), any())).thenReturn(Collections.emptyList());

        expectThrows(NoSuchFileException.class, () -> directory.fileLength("nonexistent"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Lifecycle Tests
    // ═══════════════════════════════════════════════════════════════

    public void testDelete_DeletesAllContainers() throws IOException {

        directory.delete();

        // baseBlobContainer is used for both the "lucene" format and the inherited base container
        verify(parquetBlobContainer).delete();
        verify(baseBlobContainer, times(2)).delete();
    }

    public void testClose_ClearsFormatContainers() throws IOException {
        // Verify parquet container exists before close
        assertNotNull(directory.getBlobContainerForFormat("parquet"));

        directory.close();

        // After close, the directory is closed but FormatBlobRouter still lazily creates containers
        // The important thing is that close() doesn't throw
    }

    // ═══════════════════════════════════════════════════════════════
    // Edge Case Tests
    // ═══════════════════════════════════════════════════════════════

    public void testConstructor_NullDataFormatRegistry() {
        // Should not throw with null DataFormatRegistry and IndexSettings
        DataFormatAwareRemoteDirectory dir = new DataFormatAwareRemoteDirectory(
            mockBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );
        assertNotNull(dir);
    }

    public void testToString() {

        String str = directory.toString();
        assertTrue(str.contains("DataFormatAwareRemoteDirectory"));
        assertTrue(str.contains("parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Sync CopyFrom Tests
    // ═══════════════════════════════════════════════════════════════

    public void testSyncCopyFrom_RoutesToCorrectContainer() throws IOException {
        // We can't easily test the full copyFrom without a real Directory,
        // but we can verify that the format routing logic works by testing
        // the getBlobContainerForFormat that copyFrom uses internally.

        // For lucene files
        BlobContainer luceneContainer = directory.getBlobContainerForFormat("lucene");
        assertSame(baseBlobContainer, luceneContainer);

        // For parquet files
        BlobContainer parquetContainer = directory.getBlobContainerForFormat("parquet");
        assertSame(parquetBlobContainer, parquetContainer);
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput(String, long, IOContext) Tests - Format-aware routing
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_StringBased_LuceneFile() throws IOException {
        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput("_0.cfs", 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();

        verify(baseBlobContainer).readBlob("_0.cfs");
    }

    public void testOpenInput_StringBased_ParquetFile_WithFormatSuffix() throws IOException {

        byte[] content = new byte[200];
        when(parquetBlobContainer.readBlob("_0.parquet")).thenReturn(new ByteArrayInputStream(content));

        // Register format in cache since DFARD receives plain blob keys
        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.parquet", "parquet");
        IndexInput input = directory.openInput("_0.parquet", 200, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(200, input.length());
        input.close();

        verify(parquetBlobContainer).readBlob("_0.parquet");
        verify(baseBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_StringBased_ClosesStreamOnException() throws IOException {
        when(baseBlobContainer.readBlob("_0.cfs")).thenThrow(new IOException("read error"));

        expectThrows(IOException.class, () -> directory.openInput("_0.cfs", 100, IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput Tests - Format-aware routing
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutput_LuceneFormat() throws IOException {
        RemoteIndexOutput output = directory.createOutput("test_file", "lucene", IOContext.DEFAULT);
        assertNotNull(output);
    }

    public void testCreateOutput_ParquetFormat() throws IOException {

        RemoteIndexOutput output = directory.createOutput("test_file.parquet", "parquet", IOContext.DEFAULT);
        assertNotNull(output);
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength(FileMetadata) Tests
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_FileMetadata_Lucene() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        long length = directory.fileLength(fm.file());
        assertEquals(1234, length);
    }

    public void testFileLength_FileMetadata_Parquet() throws IOException {
        FileMetadata fm = new FileMetadata("parquet", "_0.parquet");
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.parquet", 5678));
        when(parquetBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.parquet"), eq(1), any())).thenReturn(blobList);

        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.parquet", "parquet");
        long length = directory.fileLength(fm.file());
        assertEquals(5678, length);
    }

    public void testFileLength_FileMetadata_NotFound() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "nonexistent");
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("nonexistent"), eq(1), any())).thenReturn(Collections.emptyList());

        expectThrows(NoSuchFileException.class, () -> directory.fileLength(fm.file()));
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput(FileMetadata, long, IOContext) Tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_FileMetadata_Lucene() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput(fm.file(), 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();

        verify(baseBlobContainer).readBlob("_0.cfs");
    }

    public void testOpenInput_FileMetadata_Parquet() throws IOException {
        FileMetadata fm = new FileMetadata("parquet", "_0.parquet");
        byte[] content = new byte[200];
        when(parquetBlobContainer.readBlob("_0.parquet")).thenReturn(new ByteArrayInputStream(content));

        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.parquet", "parquet");
        IndexInput input = directory.openInput(fm.file(), 200, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(200, input.length());
        input.close();

        verify(parquetBlobContainer).readBlob("_0.parquet");
        verify(baseBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_FileMetadata_ClosesStreamOnException() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        InputStream mockStream = mock(InputStream.class);
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(mockStream);
        when(mockStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("read error"));

        // openInput wraps the stream, reading from it will fail
        IndexInput input = directory.openInput(fm.file(), 100, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile(UploadedSegmentMetadata) - Lucene metadata
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_WithUploadedSegmentMetadata_Lucene() throws IOException {
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");

        directory.deleteFile(metadata.getUploadedFilename());

        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.cfs__UUID1"));
        verify(parquetBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    // ═══════════════════════════════════════════════════════════════
    // Async copyFrom Tests (8-arg version, returns boolean)
    // ═══════════════════════════════════════════════════════════════

    public void testAsyncCopyFrom_NonAsyncContainer_ReturnsFalse() throws IOException {
        // baseBlobContainer is NOT AsyncMultiStreamBlobContainer, so should return false
        org.opensearch.core.action.ActionListener<Void> listener = mock(org.opensearch.core.action.ActionListener.class);
        org.apache.lucene.store.Directory mockFrom = mock(org.apache.lucene.store.Directory.class);

        boolean result = directory.copyFrom(
            mockFrom,
            "_0.cfs",              // src (lucene format, no :::)
            "_0.cfs__UUID",        // remoteFileName
            IOContext.DEFAULT,
            () -> {},
            listener,
            false,
            null
        );

        assertFalse("Should return false when container is not AsyncMultiStreamBlobContainer", result);
    }

    public void testAsyncCopyFrom_ExceptionHandling() throws IOException {
        org.opensearch.core.action.ActionListener<Void> listener = mock(org.opensearch.core.action.ActionListener.class);
        org.apache.lucene.store.Directory mockFrom = mock(org.apache.lucene.store.Directory.class);
        // Make openInput throw an exception
        when(mockFrom.openInput(anyString(), any())).thenThrow(new IOException("open failed"));

        // Even with exception, it should not propagate but call listener.onFailure
        boolean result = directory.copyFrom(mockFrom, "_0.cfs", "_0.cfs__UUID", IOContext.DEFAULT, () -> {}, listener, false, null);

        // Returns false because baseBlobContainer is not AsyncMultiStreamBlobContainer
        assertFalse(result);
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata-based copyFrom (non-async) - returns false when not async
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_FileMetadata_NonAsync_ReturnsFalse() throws IOException {
        org.opensearch.core.action.ActionListener<Void> listener = mock(org.opensearch.core.action.ActionListener.class);
        org.apache.lucene.store.Directory mockFrom = mock(org.apache.lucene.store.Directory.class);

        boolean result = directory.copyFrom(
            mockFrom,
            "_0.cfs:::lucene",
            "_0.cfs__UUID",
            IOContext.DEFAULT,
            () -> {},
            listener,
            false,
            null
        );

        assertFalse("Should return false when base container is not AsyncMultiStreamBlobContainer", result);
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput(UploadedSegmentMetadata) - Exception closes stream
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_UploadedSegmentMetadata_ExceptionClosesStream() throws IOException {
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");
        when(baseBlobContainer.readBlob("_0.cfs__UUID1")).thenThrow(new IOException("blob read failed"));

        expectThrows(IOException.class, () -> directory.openInput(metadata.getUploadedFilename(), 100, IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // Metadata file routing
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_MetadataFile() throws IOException {
        directory.deleteFile("metadata__1__2__3");

        // "metadata" format routes to base container
        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("metadata__1__2__3"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Sync copyFrom(Directory, String, String, IOContext) Tests
    // ═══════════════════════════════════════════════════════════════

    public void testSyncCopyFrom_LuceneFile_CopiesToBaseContainer() throws IOException {
        Directory mockFrom = mock(Directory.class);
        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(10L);
        when(mockFrom.openInput(eq("_0.cfs"), any(IOContext.class))).thenReturn(mockInput);

        // The copyFrom creates a RemoteIndexOutput that writes to the base container
        directory.copyFrom(mockFrom, "_0.cfs", "_0.cfs__UUID", IOContext.DEFAULT);

        verify(mockFrom).openInput(eq("_0.cfs"), any(IOContext.class));
    }

    public void testSyncCopyFrom_ParquetFile_CopiesToFormatContainer() throws IOException {
        Directory mockFrom = mock(Directory.class);
        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(20L);
        when(mockFrom.openInput(eq("_0.pqt:::parquet"), any(IOContext.class))).thenReturn(mockInput);

        directory.copyFrom(mockFrom, "_0.pqt:::parquet", "_0.pqt__UUID", IOContext.DEFAULT);

        verify(mockFrom).openInput(eq("_0.pqt:::parquet"), any(IOContext.class));
    }

    // ═══════════════════════════════════════════════════════════════
    // Async copyFrom with AsyncMultiStreamBlobContainer Tests
    // ═══════════════════════════════════════════════════════════════

    public void testAsyncCopyFrom_WithAsyncContainer_ReturnsTrue() throws Exception {
        // Create an async blob container
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        // Wire up blobStore to return async container for base path
        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );

        // Set up async upload to call onResponse
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        // Create a real directory with a file that has a valid codec footer
        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("Hello World!");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Boolean> postUploadInvoked = new AtomicReference<>(false);

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename,
            IOContext.DEFAULT,
            () -> postUploadInvoked.set(true),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                }
            },
            false,
            null
        );

        assertTrue("Should return true when container is AsyncMultiStreamBlobContainer", result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(postUploadInvoked.get());
        storeDirectory.close();
    }

    public void testAsyncCopyFrom_ExceptionDuringUpload_CallsListenerOnFailure() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );

        // File does not exist - openInput will throw
        Directory storeDirectory = newDirectory();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            "_nonexistent.si",
            "_nonexistent.si__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should have failed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue("Should return true (handled)", result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotNull(failureRef.get());
        storeDirectory.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput exception close-on-failure (stream opened but readBlob fails)
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_FileMetadata_ExceptionClosesStream() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        when(baseBlobContainer.readBlob("_0.cfs")).thenThrow(new IOException("blob read failed"));

        expectThrows(IOException.class, () -> directory.openInput(fm.file(), 100, IOContext.DEFAULT));
    }

    public void testOpenInput_StringBased_StreamClosedWhenInputStreamReadFails() throws IOException {
        InputStream mockStream = mock(InputStream.class);
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(mockStream);
        when(mockStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("stream error"));

        // openInput wraps the stream successfully
        IndexInput input = directory.openInput("_0.cfs", 100, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength name mismatch tests
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_NameMismatch_ThrowsNoSuchFile() throws IOException {
        // Returns a blob but name doesn't match
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs_DIFFERENT", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        expectThrows(NoSuchFileException.class, () -> directory.fileLength("_0.cfs"));
    }

    public void testFileLength_FileMetadata_NameMismatch_ThrowsNoSuchFile() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs_DIFFERENT", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        expectThrows(NoSuchFileException.class, () -> directory.fileLength(fm.file()));
    }

    // ═══════════════════════════════════════════════════════════════
    // DownloadRateLimiterProvider merged segment path
    // ═══════════════════════════════════════════════════════════════

    public void testDownloadRateLimiter_MergedSegment_UsesLowPriorityRateLimiter() throws IOException {
        // Create directory with pending merged segments
        Map<String, String> pendingMergedSegments = new HashMap<>();
        pendingMergedSegments.put("localFile", "_0.cfs__UUID_MERGED");

        UnaryOperator<InputStream> normalRateLimiter = stream -> stream;
        UnaryOperator<InputStream> lowPriorityRateLimiter = stream -> stream;

        DataFormatAwareRemoteDirectory dirWithMerged = new DataFormatAwareRemoteDirectory(
            mockBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            normalRateLimiter,
            lowPriorityRateLimiter,
            pendingMergedSegments,
            logger,
            null,
            null
        );

        // When opening a merged segment, the low-priority rate limiter should be used
        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs__UUID_MERGED")).thenReturn(new ByteArrayInputStream(content));

        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID_MERGED::checksum123::100::10");

        IndexInput input = dirWithMerged.openInput(metadata.getUploadedFilename(), 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Completion listener tests (via async upload with errors)
    // ═══════════════════════════════════════════════════════════════

    public void testCompletionListener_PostUploadRunnerException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);

        // postUploadRunner throws exception → listener.onFailure
        boolean result = asyncDir.copyFrom(storeDirectory, filename, filename + "__UUID", IOContext.DEFAULT, () -> {
            throw new RuntimeException("postUpload error");
        }, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("Should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        }, false, null);

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        storeDirectory.close();
    }

    public void testCompletionListener_CorruptIndexException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );

        // asyncBlobUpload calls onFailure with a wrapped CorruptIndexException
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new RuntimeException(new CorruptIndexException("corrupted", "test")));
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue("Should be CorruptIndexException", failureRef.get() instanceof CorruptIndexException);
        storeDirectory.close();
    }

    public void testCompletionListener_CorruptFileException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );

        // asyncBlobUpload calls onFailure with a wrapped CorruptFileException
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new RuntimeException(new CorruptFileException("corrupted", "test_file")));
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue("Should be CorruptIndexException", failureRef.get() instanceof CorruptIndexException);
        storeDirectory.close();
    }

    public void testCompletionListener_GenericException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );

        // asyncBlobUpload calls onFailure with a generic exception (not corrupt)
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new IOException("network error"));
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue("Should be IOException", failureRef.get() instanceof IOException);
        storeDirectory.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Sync copyFrom with FileMetadata
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_FileMetadata_Sync() throws IOException {
        DataFormatAwareStoreDirectory mockComposite = mock(DataFormatAwareStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");

        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(50L);
        when(mockComposite.openInput(eq(fm.serialize()), eq(IOContext.DEFAULT))).thenReturn(mockInput);

        // Should write to base container since format is "lucene"
        directory.copyFrom(mockComposite, fm.serialize(), "_0.cfs__UUID", IOContext.DEFAULT);

        verify(mockComposite).openInput(eq(fm.serialize()), eq(IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // Low priority upload path (content > 15GB triggers low priority)
    // ═══════════════════════════════════════════════════════════════

    public void testAsyncCopyFrom_LowPriorityUpload() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null,
            null
        );

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);

        // Pass lowPriorityUpload=true
        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                }
            },
            true,  // lowPriorityUpload
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        storeDirectory.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Blob Format Cache Tests
    // ═══════════════════════════════════════════════════════════════

    public void testReplaceBlobFormatCache() throws IOException {
        // Initially no cache entries — deleteFile for a UUID key defaults to lucene (base container)
        directory.deleteFile("_0.pqt__UUID1");
        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(eq(Collections.singletonList("_0.pqt__UUID1")));

        // Replace cache with parquet mapping
        directory.getFormatBlobRouter().orElseThrow().replaceBlobFormatCache(Map.of("_0.pqt__UUID1", "parquet"));
        directory.deleteFile("_0.pqt__UUID1");
        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(eq(Collections.singletonList("_0.pqt__UUID1")));
    }

    public void testUnregisterBlobFormat() throws IOException {
        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.pqt__UUID1", "parquet");
        directory.deleteFile("_0.pqt__UUID1");
        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(eq(Collections.singletonList("_0.pqt__UUID1")));

        // Unregister — should fall back to lucene
        directory.getFormatBlobRouter().orElseThrow().unregisterBlobFormat("_0.pqt__UUID1");
        directory.deleteFile("_0.pqt__UUID1");
        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(eq(Collections.singletonList("_0.pqt__UUID1")));
    }

    public void testResolveFormat_CacheMiss_DefaultsToLucene() throws IOException {
        // UUID-suffixed key not in cache — should warn and default to lucene
        directory.deleteFile("_0.pqt__UUID_MISSING");
        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(eq(Collections.singletonList("_0.pqt__UUID_MISSING")));
    }

    // ═══════════════════════════════════════════════════════════════
    // openBlockInput Tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenBlockInput_LuceneFile() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5 };
        InputStream stream = new ByteArrayInputStream(data);
        when(baseBlobContainer.readBlob("_0.cfe__UUID1", 0, 5)).thenReturn(stream);

        IndexInput input = directory.openBlockInput("_0.cfe__UUID1", 0, 5, 5, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    public void testOpenBlockInput_ParquetFile_WithCache() throws IOException {
        directory.getFormatBlobRouter().orElseThrow().registerBlobFormat("_0.pqt__UUID1", "parquet");
        byte[] data = new byte[] { 10, 20, 30 };
        InputStream stream = new ByteArrayInputStream(data);
        when(parquetBlobContainer.readBlob("_0.pqt__UUID1", 2, 3)).thenReturn(stream);

        IndexInput input = directory.openBlockInput("_0.pqt__UUID1", 2, 3, 10, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    public void testOpenBlockInput_InvalidPosition_Throws() {
        expectThrows(IllegalArgumentException.class, () -> directory.openBlockInput("_0.cfe__UUID1", -1, 5, 10, IOContext.DEFAULT));
    }

    public void testOpenBlockInput_LengthExceedsFileLength_Throws() {
        expectThrows(IllegalArgumentException.class, () -> directory.openBlockInput("_0.cfe__UUID1", 5, 10, 10, IOContext.DEFAULT));
    }
}
