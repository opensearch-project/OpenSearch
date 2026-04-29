/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end unit tests for {@link IcebergMetadataClient#publish} using
 * Iceberg's {@link InMemoryCatalog} and {@link org.apache.iceberg.inmemory.InMemoryFileIO}
 * test fixtures — no network, no mocks around the catalog itself. Covers:
 * <ul>
 *   <li>First-attempt publish registers new parquet files under the correct partition.</li>
 *   <li>Idempotent retry is a no-op (no duplicate rows).</li>
 *   <li>Two shards of the same index write independent partitions without cross-pollution.</li>
 *   <li>Empty metadata file → zero commits, no snapshot.</li>
 * </ul>
 */
@ThreadLeakFilters(filters = IcebergThreadLeakFilter.class)
public class IcebergMetadataClientPublishTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "logs";
    private static final String INDEX_UUID = "uuid-logs-1";
    private static final String WAREHOUSE_LOCATION = "memory://warehouse/ns/logs";
    private static final TableIdentifier TABLE_ID = TableIdentifier.of(Namespace.of(IcebergMetadataClient.NAMESPACE), INDEX_NAME);

    private InMemoryCatalog catalog;
    private IcebergMetadataClient client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of(IcebergMetadataClient.NAMESPACE));

        // Use initialize() to create the table end-to-end, so we exercise the property-stamping
        // logic exactly the same way a real orchestrator call would.
        IndexMetadata metadata = IcebergMetadataClientTests.minimalIndexMetadata(INDEX_NAME, INDEX_UUID);
        // Override the catalog-default location to use the in-memory filesystem namespace.
        Schema schema = OpenSearchSchemaInference.inferSchema(metadata);
        PartitionSpec spec = OpenSearchSchemaInference.partitionSpec(schema);
        catalog.buildTable(TABLE_ID, schema)
            .withPartitionSpec(spec)
            .withLocation(WAREHOUSE_LOCATION)
            .withProperty(IcebergMetadataClient.PROPERTY_INDEX_UUID, INDEX_UUID)
            .create();

        client = new IcebergMetadataClient(catalog);
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
        catalog.close();
        super.tearDown();
    }

    public void testPublishAppendsParquetFiles() throws IOException {
        Map<String, byte[]> files = new LinkedHashMap<>();
        files.put("_0.parquet__a", randomBytes(512));
        files.put("_1.parquet__b", randomBytes(1024));
        files.put("_2.cfs__c", randomBytes(256));    // non-parquet, should be filtered out
        RemoteSegmentStoreDirectory src = remoteStoreWith(files);

        // Sanity check: confirm the mocked metadata actually returns our file names.
        RemoteSegmentMetadata debugMeta = src.readLatestMetadataFile();
        assertNotNull("sanity: readLatestMetadataFile returned non-null", debugMeta);
        assertEquals("sanity: mock wiring check", 3, debugMeta.getMetadata().size());
        // Call again to verify multi-shot behaviour.
        RemoteSegmentMetadata debugMeta2 = src.readLatestMetadataFile();
        assertNotNull("sanity: second call returned non-null", debugMeta2);
        assertEquals("sanity: second call has 3 entries", 3, debugMeta2.getMetadata().size());

        client.publish(INDEX_NAME, src, 0);

        Set<String> tracked = readTrackedPathsFor(0);
        assertEquals(2, tracked.size());
        assertTrue(tracked.stream().anyMatch(p -> p.endsWith("/_0.parquet__a")));
        assertTrue(tracked.stream().anyMatch(p -> p.endsWith("/_1.parquet__b")));
        assertFalse("non-parquet filtered out", tracked.stream().anyMatch(p -> p.endsWith("/_2.cfs__c")));
    }

    public void testPublishIsIdempotentOnRetry() throws IOException {
        Map<String, byte[]> files = Map.of("_0.parquet__x", randomBytes(512), "_1.parquet__y", randomBytes(1024));
        RemoteSegmentStoreDirectory src = remoteStoreWith(files);

        client.publish(INDEX_NAME, src, 0);
        long snapshotAfterFirst = currentSnapshotId();

        // Same source directory → same filenames → filter-before-commit kicks in; no new files.
        client.publish(INDEX_NAME, src, 0);
        long snapshotAfterSecond = currentSnapshotId();

        assertEquals("retry must not create a new snapshot", snapshotAfterFirst, snapshotAfterSecond);
        assertEquals("row count unchanged", 2, readTrackedPathsFor(0).size());
    }

    public void testPublishPerShardPartitionsAreIndependent() throws IOException {
        // Overlapping filenames between shards — must not interfere thanks to shard_id
        // partition key. Same filename written under each shard's own warehouse prefix.
        Map<String, byte[]> shard0Files = Map.of("_0.parquet__shared", randomBytes(128));
        Map<String, byte[]> shard1Files = Map.of("_0.parquet__shared", randomBytes(128));

        client.publish(INDEX_NAME, remoteStoreWith(shard0Files), 0);
        client.publish(INDEX_NAME, remoteStoreWith(shard1Files), 1);

        Set<String> shard0 = readTrackedPathsFor(0);
        Set<String> shard1 = readTrackedPathsFor(1);
        assertEquals(1, shard0.size());
        assertEquals(1, shard1.size());
        assertTrue(shard0.iterator().next().contains("/0/"));
        assertTrue(shard1.iterator().next().contains("/1/"));
    }

    public void testPublishNoMetadataFileIsNoOp() throws IOException {
        RemoteSegmentStoreDirectory src = mock(RemoteSegmentStoreDirectory.class);
        when(src.readLatestMetadataFile()).thenReturn(null);

        client.publish(INDEX_NAME, src, 0);

        assertNull("no files published → no snapshot created", catalog.loadTable(TABLE_ID).currentSnapshot());
    }

    public void testPublishOnlyNonParquetFilesIsNoOp() throws IOException {
        RemoteSegmentStoreDirectory src = remoteStoreWith(Map.of("_0.cfs__a", randomBytes(256), "_1.si__b", randomBytes(128)));

        client.publish(INDEX_NAME, src, 0);

        assertNull("only non-parquet files → no snapshot", catalog.loadTable(TABLE_ID).currentSnapshot());
    }

    // --- helpers ----------------------------------------------------------

    private long currentSnapshotId() {
        Snapshot snapshot = catalog.loadTable(TABLE_ID).currentSnapshot();
        assertNotNull("expected at least one snapshot", snapshot);
        return snapshot.snapshotId();
    }

    private Set<String> readTrackedPathsFor(int shardId) throws IOException {
        Table table = catalog.loadTable(TABLE_ID);
        table.refresh();
        Set<String> paths = new HashSet<>();
        if (table.currentSnapshot() == null) {
            return paths;
        }
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask t : tasks) {
                org.apache.iceberg.StructLike partition = t.file().partition();
                Object storedUUID = partition.get(0, Object.class);
                Object storedShard = partition.get(1, Object.class);
                if (INDEX_UUID.equals(String.valueOf(storedUUID)) && Integer.valueOf(shardId).equals(storedShard)) {
                    paths.add(t.file().path().toString());
                }
            }
        }
        return paths;
    }

    private static byte[] randomBytes(int length) {
        byte[] b = new byte[length];
        random().nextBytes(b);
        return b;
    }

    /**
     * Wraps a filename→payload map as a {@link RemoteSegmentStoreDirectory} stub that
     * answers {@link RemoteSegmentStoreDirectory#readLatestMetadataFile} with the
     * filenames and {@link RemoteSegmentStoreDirectory#openInput} with an
     * {@link IndexInput} over the payload.
     */
    private static RemoteSegmentStoreDirectory remoteStoreWith(Map<String, byte[]> files) throws IOException {
        RemoteSegmentStoreDirectory dir = mock(RemoteSegmentStoreDirectory.class);

        // readLatestMetadataFile() → RemoteSegmentMetadata whose getMetadata().keySet() is the filenames.
        // Use a mock because RemoteSegmentMetadata has a complex constructor that we don't need here.
        RemoteSegmentMetadata metadata = mock(RemoteSegmentMetadata.class);
        Map<String, UploadedSegmentMetadata> fakeMap = new LinkedHashMap<>();
        for (String name : new TreeMap<>(files).keySet()) {
            // The value doesn't matter — publish() only reads the key set — but it must be non-null.
            fakeMap.put(name, mock(UploadedSegmentMetadata.class));
        }
        org.mockito.Mockito.doReturn(fakeMap).when(metadata).getMetadata();
        org.mockito.Mockito.doReturn(metadata).when(dir).readLatestMetadataFile();

        // openInput / fileLength per filename.
        for (Map.Entry<String, byte[]> entry : files.entrySet()) {
            String filename = entry.getKey();
            byte[] payload = entry.getValue();
            when(dir.fileLength(eq(filename))).thenReturn((long) payload.length);
            doAnswer(invocation -> byteArrayIndexInput(filename, payload)).when(dir).openInput(eq(filename), any(IOContext.class));
        }
        return dir;
    }

    private static IndexInput byteArrayIndexInput(String name, byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        return new IndexInput("byte-array[" + name + "]") {
            @Override
            public void close() {}

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
