/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ParquetFileUploader}. Exercises the layer-3 guarantee
 * (deterministic warehouse path + streaming copy) via an in-memory Iceberg
 * {@link InMemoryCatalog} + {@link InMemoryFileIO}, with the source
 * {@link RemoteSegmentStoreDirectory} mocked to return a deterministic byte
 * stream.
 */
@ThreadLeakFilters(filters = IcebergThreadLeakFilter.class)
public class ParquetFileUploaderTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "my-index";
    private static final String INDEX_UUID = "uuid-abc";
    private static final int SHARD_ID = 2;
    private static final String WAREHOUSE_LOCATION = "memory://warehouse/ns/my-index";
    private static final TableIdentifier TABLE_ID = TableIdentifier.of(Namespace.of(IcebergMetadataClient.NAMESPACE), INDEX_NAME);

    private InMemoryCatalog catalog;
    private InMemoryFileIO fileIO;
    private PublishContext ctx;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of(IcebergMetadataClient.NAMESPACE));
        Schema schema = OpenSearchSchemaInference.inferSchema(IcebergMetadataClientTests.minimalIndexMetadata(INDEX_NAME, INDEX_UUID));
        PartitionSpec spec = OpenSearchSchemaInference.partitionSpec(schema);
        Table table = catalog.buildTable(TABLE_ID, schema)
            .withPartitionSpec(spec)
            .withLocation(WAREHOUSE_LOCATION)
            .withProperty(IcebergMetadataClient.PROPERTY_INDEX_UUID, INDEX_UUID)
            .create();
        fileIO = (InMemoryFileIO) table.io();
        ctx = PublishContext.create(catalog, TABLE_ID, INDEX_NAME, SHARD_ID);
    }

    @Override
    public void tearDown() throws Exception {
        catalog.close();
        super.tearDown();
    }

    public void testUploadSmallFileRoundTripsBytes() throws IOException {
        byte[] payload = randomBytes(1024); // 1 KB, fits in one copy-buffer chunk
        String filename = "_0.parquet__small-UUID";
        RemoteSegmentStoreDirectory src = directoryReturning(filename, payload);

        DataFile df = ParquetFileUploader.upload(ctx, src, filename, fileIO);

        assertEquals("data/" + INDEX_UUID + "/" + SHARD_ID + "/" + filename, stripTableLocationPrefix(df.path().toString()));
        assertEquals((long) payload.length, df.fileSizeInBytes());
        assertEquals(FileFormat.PARQUET, df.format());
        assertEquals("record count stub per design doc", 0L, df.recordCount());
        assertArrayEquals(payload, readBackWrittenBytes(df.path().toString()));
    }

    public void testUploadLargeFileChunksWithoutTruncation() throws IOException {
        // 9 MB > 8 MB buffer; ensures the copy loop runs >1 iteration and doesn't drop bytes.
        byte[] payload = randomBytes(9 * 1024 * 1024 + 37); // odd size so final chunk isn't buffer-aligned
        String filename = "_1.parquet__big-UUID";
        RemoteSegmentStoreDirectory src = directoryReturning(filename, payload);

        DataFile df = ParquetFileUploader.upload(ctx, src, filename, fileIO);

        assertEquals((long) payload.length, df.fileSizeInBytes());
        assertArrayEquals(payload, readBackWrittenBytes(df.path().toString()));
    }

    public void testUploadIsDeterministicOnRetry() throws IOException {
        // Same source filename + same content → same warehouse key + same bytes. Rerunning
        // the upload overwrites with identical content (S3 PUT is idempotent on key).
        byte[] payload = randomBytes(2048);
        String filename = "_2.parquet__retry-UUID";
        RemoteSegmentStoreDirectory src = directoryReturning(filename, payload);

        DataFile first = ParquetFileUploader.upload(ctx, src, filename, fileIO);
        DataFile second = ParquetFileUploader.upload(ctx, src, filename, fileIO);

        assertEquals("warehouse path is deterministic across retries", first.path(), second.path());
        assertArrayEquals(payload, readBackWrittenBytes(second.path().toString()));
    }

    public void testSourceExceptionPropagates() throws IOException {
        String filename = "_3.parquet__err-UUID";
        RemoteSegmentStoreDirectory src = mock(RemoteSegmentStoreDirectory.class);
        when(src.fileLength(eq(filename))).thenReturn(16L);
        when(src.openInput(eq(filename), any(IOContext.class))).thenThrow(new IOException("boom"));

        expectThrows(IOException.class, () -> ParquetFileUploader.upload(ctx, src, filename, fileIO));
    }

    // --- helpers ----------------------------------------------------------

    private static byte[] randomBytes(int length) {
        byte[] b = new byte[length];
        random().nextBytes(b);
        return b;
    }

    /** Strips the table location prefix so asserts focus on the warehouse-key suffix. */
    private static String stripTableLocationPrefix(String absolutePath) {
        String prefix = WAREHOUSE_LOCATION + "/";
        assertTrue("expected path [" + absolutePath + "] to start with [" + prefix + "]", absolutePath.startsWith(prefix));
        return absolutePath.substring(prefix.length());
    }

    private byte[] readBackWrittenBytes(String absolutePath) throws IOException {
        byte[] out = new byte[(int) fileIO.newInputFile(absolutePath).getLength()];
        try (var in = fileIO.newInputFile(absolutePath).newStream()) {
            int read = 0;
            while (read < out.length) {
                int n = in.read(out, read, out.length - read);
                if (n < 0) {
                    break;
                }
                read += n;
            }
        }
        return out;
    }

    /**
     * Builds a {@link RemoteSegmentStoreDirectory} mock that, on
     * {@link RemoteSegmentStoreDirectory#openInput} for {@code filename}, returns an
     * {@link IndexInput} wrapping {@code payload}.
     */
    private static RemoteSegmentStoreDirectory directoryReturning(String filename, byte[] payload) throws IOException {
        RemoteSegmentStoreDirectory dir = mock(RemoteSegmentStoreDirectory.class);
        when(dir.fileLength(eq(filename))).thenReturn((long) payload.length);
        doAnswer(invocation -> byteArrayIndexInput(filename, payload)).when(dir).openInput(eq(filename), any(IOContext.class));
        return dir;
    }

    /** Minimal IndexInput implementation backed by a byte[], enough to support readBytes. */
    private static IndexInput byteArrayIndexInput(String name, byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        return new IndexInput("byte-array-index-input[" + name + "]") {
            @Override
            public void close() {
                // no-op
            }

            @Override
            public long getFilePointer() {
                return buf.position();
            }

            @Override
            public void seek(long pos) {
                buf.position(Math.toIntExact(pos));
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public IndexInput slice(String sliceDescription, long offset, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte readByte() {
                return buf.get();
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) {
                buf.get(b, offset, len);
            }
        };
    }
}
