/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;

/**
 * Uploads a single parquet file from the OpenSearch remote store to the Iceberg
 * warehouse and builds the corresponding {@link DataFile} registration entry.
 * <p>
 * Implements the layer-3 idempotency primitive: warehouse keys are derived
 * deterministically from the remote-store filename (see
 * {@link WarehousePathResolver}), so retries overwrite the same S3 key with the
 * same bytes — {@code PutObject} / {@code CompleteMultipartUpload} is idempotent
 * on key when the content is identical.
 * <p>
 * The copy uses an 8 MB heap buffer, matching the AWS SDK v2 transfer-manager
 * default part size. Heap footprint is bounded regardless of segment size.
 *
 * <p><b>Record count stub.</b> The produced {@link DataFile} currently has
 * {@code recordCount = 0}. Reading the true count requires either (a) pulling
 * in {@code iceberg-parquet} plus ~12 transitive jars (parquet-mr, avro, codec
 * libs) and their SHA/LICENSE/thirdPartyAudit bookkeeping, or (b) hand-rolling
 * a thrift footer decoder against the parquet {@code FileMetaData} struct. Both
 * are deferred — see PR 8c in the spec. External query engines (DataFusion)
 * read the footer themselves during query planning, so the missing count is a
 * minor first-touch planning-latency concern, not a correctness issue.
 */
final class ParquetFileUploader {

    private static final Logger logger = LogManager.getLogger(ParquetFileUploader.class);

    /**
     * Copy buffer size. Matches the AWS SDK v2 transfer-manager default part
     * size so a single heap buffer feeds the multipart upload machinery without
     * re-chunking.
     */
    static final int COPY_BUFFER_BYTES = 8 * 1024 * 1024;

    private ParquetFileUploader() {}

    /**
     * Uploads a single parquet file and returns its {@link DataFile} entry.
     *
     * @param ctx           per-invocation context (table, partition values, absolute-path helper)
     * @param remoteDirectory source Lucene directory backed by the remote store
     * @param remoteFilename logical remote-store filename (already validated to be parquet)
     * @param fileIO        warehouse-side IO (typically the table's {@code S3FileIO})
     * @return a {@code DataFile} describing the uploaded file. {@code recordCount = 0}
     *         is intentional; see the class javadoc.
     * @throws IOException if either the source read or the destination write fails.
     *                     Any partial S3 object becomes a warehouse orphan for
     *                     out-of-band cleanup via {@code removeOrphanFiles}.
     */
    static DataFile upload(PublishContext ctx, RemoteSegmentStoreDirectory remoteDirectory, String remoteFilename, FileIO fileIO)
        throws IOException {
        String warehouseKey = WarehousePathResolver.resolve(ctx.indexUUID(), ctx.shardId(), remoteFilename);
        String absolutePath = ctx.absoluteWarehousePath(warehouseKey);
        long fileLength = remoteDirectory.fileLength(remoteFilename);

        OutputFile outputFile = fileIO.newOutputFile(absolutePath);
        try (
            IndexInput in = remoteDirectory.openInput(remoteFilename, IOContext.READONCE);
            PositionOutputStream out = outputFile.createOrOverwrite()
        ) {
            copy(in, out, fileLength);
        }

        logger.debug(
            "Uploaded [{}] ({} bytes) for shard [{}][{}] to [{}]",
            remoteFilename,
            fileLength,
            ctx.indexName(),
            ctx.shardId(),
            absolutePath
        );

        PartitionKey partitionKey = new PartitionKey(ctx.table().spec(), ctx.table().schema());
        partitionKey.partition(partitionRecord(ctx));

        return DataFiles.builder(ctx.table().spec())
            .withPath(absolutePath)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(fileLength)
            // Record count stub: see class javadoc. Deferred to PR 8c.
            .withRecordCount(0L)
            .withPartition(partitionKey)
            .build();
    }

    private static void copy(IndexInput in, PositionOutputStream out, long length) throws IOException {
        byte[] buf = new byte[COPY_BUFFER_BYTES];
        long remaining = length;
        while (remaining > 0) {
            int chunk = (int) Math.min((long) buf.length, remaining);
            in.readBytes(buf, 0, chunk);
            out.write(buf, 0, chunk);
            remaining -= chunk;
        }
    }

    /**
     * Returns an Iceberg record carrying this publish's partition values
     * — {@code (index_uuid, shard_id)}. The record is transient; it exists only
     * to drive {@link PartitionKey#partition(org.apache.iceberg.StructLike)}.
     */
    private static GenericRecord partitionRecord(PublishContext ctx) {
        GenericRecord record = GenericRecord.create(ctx.table().schema());
        record.setField(OpenSearchSchemaInference.FIELD_INDEX_UUID, ctx.indexUUID());
        record.setField(OpenSearchSchemaInference.FIELD_SHARD_ID, ctx.shardId());
        return record;
    }
}
