/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import io.trino.hive.thrift.metastore.Partition;
import io.trino.hive.thrift.metastore.ThriftHiveMetastore;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Shard consumer that reads from Hive tables. Each shard independently queries
 * Hive Metastore for new partitions (incremental fetch) and reads assigned data files.
 */
@SuppressWarnings("removal")
public class HiveShardConsumer implements IngestionShardConsumer<HivePointer, HiveMessage> {

    private static final Logger logger = LogManager.getLogger(HiveShardConsumer.class);

    private final String clientId;
    private final int shardId;
    private final HiveSourceConfig config;
    private final int numShards;

    // Metastore and filesystem
    private ThriftHiveMetastore.Client metastoreClient;
    private TTransport metastoreTransport;
    private FileSystem fileSystem;
    private Configuration hadoopConf;

    // Partition tracking (Flink ContinuousPartitionFetcher pattern)
    private String watermark;  // last fully processed partition name
    private long lastMetastoreQueryTime;

    // Current processing state
    private List<PartitionWork> pendingWork;
    private int currentWorkIndex;
    private ParquetFileReader currentFileReader;
    private MessageType currentSchema;
    private PageReadStore currentRowGroup;
    private RecordReader<Group> currentRecordReader;
    private long currentRowGroupRowsRemaining;
    private String currentFile;
    private long currentRowIndex;
    private long sequenceNumber;
    private final java.util.Set<String> seenPartitions = new java.util.HashSet<>();

    public HiveShardConsumer(String clientId, int shardId, HiveSourceConfig config) {
        this.clientId = clientId;
        this.shardId = shardId;
        this.config = config;
        this.numShards = config.getNumShards();
        this.watermark = config.getConsumeStartOffset();
        this.pendingWork = new ArrayList<>();
        this.currentWorkIndex = 0;
        this.sequenceNumber = 0;
        this.lastMetastoreQueryTime = 0;
    }

    private void ensureInitialized() throws Exception {
        if (metastoreClient == null) {
            logger.info("Initializing HiveShardConsumer for shard {} with metastore URI: {}", shardId, config.getMetastoreUri());

            // Parse thrift://host:port
            String uri = config.getMetastoreUri().replace("thrift://", "");
            String[] hostPort = uri.split(":");
            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);

            logger.info("Attempting to connect to Hive Metastore for shard {} at {}:{}", shardId, host, port);
            metastoreTransport = new TSocket(host, port, 10000);
            metastoreTransport.open();
            metastoreClient = new ThriftHiveMetastore.Client(new TBinaryProtocol(metastoreTransport));

            hadoopConf = java.security.AccessController.doPrivileged(
                (java.security.PrivilegedExceptionAction<Configuration>) () -> {
                    ClassLoader original = Thread.currentThread().getContextClassLoader();
                    try {
                        Thread.currentThread().setContextClassLoader(HiveShardConsumer.class.getClassLoader());
                        Configuration conf = new Configuration();
                        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
                        return conf;
                    } finally {
                        Thread.currentThread().setContextClassLoader(original);
                    }
                }
            );
            fileSystem = java.security.AccessController.doPrivileged(
                (java.security.PrivilegedExceptionAction<FileSystem>) () -> {
                    ClassLoader original = Thread.currentThread().getContextClassLoader();
                    try {
                        Thread.currentThread().setContextClassLoader(HiveShardConsumer.class.getClassLoader());
                        return FileSystem.get(hadoopConf);
                    } finally {
                        Thread.currentThread().setContextClassLoader(original);
                    }
                }
            );
            logger.info("HiveShardConsumer initialized successfully for shard {}", shardId);
        }
    }

    @Override
    public List<ReadResult<HivePointer, HiveMessage>> readNext(HivePointer pointer, boolean includeStart, long maxMessages, int timeoutMillis) {
        try {
            return doReadNext(maxMessages);
        } catch (Throwable e) {
            logger.error("Error reading from Hive table for shard " + shardId, e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<ReadResult<HivePointer, HiveMessage>> readNext(long maxMessages, int timeoutMillis) {
        try {
            return doReadNext(maxMessages);
        } catch (Throwable e) {
            logger.error("Error reading from Hive table for shard " + shardId, e);
            return Collections.emptyList();
        }
    }

    private List<ReadResult<HivePointer, HiveMessage>> doReadNext(long maxMessages) throws Exception {
        ensureInitialized();

        logger.debug("doReadNext called for shard {}, shouldRefresh={}, pendingWork={}, currentFileReader={}",
            shardId, shouldRefreshPartitions(), pendingWork.size(), currentFileReader != null);

        // Check if we should query Metastore for new partitions
        if (shouldRefreshPartitions()) {
            discoverNewPartitions();
        }

        // No work to do
        if (currentWorkIndex >= pendingWork.size() && currentFileReader == null) {
            return Collections.emptyList();
        }

        // Open next file if needed
        if (currentFileReader == null) {
            if (!openNextFile()) {
                return Collections.emptyList();
            }
        }

        // Read rows
        List<ReadResult<HivePointer, HiveMessage>> results = new ArrayList<>();
        long count = 0;
        while (count < maxMessages) {
            // Need next row group?
            if (currentRecordReader == null || currentRowGroupRowsRemaining <= 0) {
                currentRowGroup = currentFileReader.readNextRowGroup();
                if (currentRowGroup == null) {
                    // File exhausted, move to next
                    closeCurrentReader();
                    if (!openNextFile()) {
                        break;
                    }
                    continue;
                }
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(currentSchema);
                currentRecordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(currentSchema));
                currentRowGroupRowsRemaining = currentRowGroup.getRowCount();
            }

            Group record = currentRecordReader.read();
            currentRowGroupRowsRemaining--;

            byte[] json = recordToJson(record);
            PartitionWork work = pendingWork.get(currentWorkIndex);
            HivePointer ptr = new HivePointer(work.partitionName, currentFile, currentRowIndex, sequenceNumber);
            HiveMessage msg = new HiveMessage(json, System.currentTimeMillis());
            results.add(new ReadResult<>(ptr, msg));
            currentRowIndex++;
            sequenceNumber++;
            count++;
        }
        return results;
    }

    /**
     * Incremental partition fetch: query Metastore for partitions added after watermark.
     * Follows the same pattern as Flink's ContinuousPartitionFetcher.fetchPartitions().
     */
    private void discoverNewPartitions() throws Exception {
        List<Partition> partitions;
        if (watermark == null || watermark.isEmpty()) {
            // First time: get all partitions
            partitions = metastoreClient.getPartitions(config.getDatabase(), config.getTable(), (short) -1);
        } else {
            // Incremental: filter by partition value > watermark
            String filter = buildPartitionFilter(watermark);
            partitions = metastoreClient.getPartitionsByFilter(config.getDatabase(), config.getTable(), filter, (short) -1);
        }

        logger.info("Shard {} discovered {} partitions from Metastore", shardId, partitions.size());

        // Sort by partition value
        partitions.sort((a, b) -> {
            String aVal = String.join("/", a.getValues());
            String bVal = String.join("/", b.getValues());
            return aVal.compareTo(bVal);
        });

        // Filter to partitions assigned to this shard
        for (Partition partition : partitions) {
            String partName = partitionToName(partition);
            if (seenPartitions.contains(partName)) continue;
            if (Math.floorMod(partName.hashCode(), numShards) == shardId) {
                // List files in this partition
                String location = partition.getSd().getLocation();
                List<String> files = listDataFiles(location);
                if (!files.isEmpty()) {
                    pendingWork.add(new PartitionWork(partName, files));
                    seenPartitions.add(partName);
                    logger.info("Shard {} assigned partition {} with {} files", shardId, partName, files.size());
                }
            }
        }

        lastMetastoreQueryTime = System.currentTimeMillis();
    }

    private boolean shouldRefreshPartitions() {
        if (lastMetastoreQueryTime == 0) return true;
        // Only refresh if we have no pending work and interval has elapsed
        boolean noPendingWork = currentWorkIndex >= pendingWork.size() && currentFileReader == null;
        boolean intervalElapsed = System.currentTimeMillis() - lastMetastoreQueryTime >= config.getMonitorIntervalMillis();
        return noPendingWork && intervalElapsed;
    }

    private boolean openNextFile() throws IOException {
        while (currentWorkIndex < pendingWork.size()) {
            PartitionWork work = pendingWork.get(currentWorkIndex);
            if (work.currentFileIndex < work.files.size()) {
                currentFile = work.files.get(work.currentFileIndex);
                work.currentFileIndex++;
                currentRowIndex = 0;
                currentFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(currentFile), hadoopConf));
                currentSchema = currentFileReader.getFooter().getFileMetaData().getSchema();
                currentRecordReader = null;
                currentRowGroupRowsRemaining = 0;
                return true;
            } else {
                // Partition complete, update watermark
                watermark = work.partitionName;
                currentWorkIndex++;
            }
        }
        return false;
    }

    private void closeCurrentReader() {
        if (currentFileReader != null) {
            try {
                currentFileReader.close();
            } catch (IOException e) {
                logger.warn("Error closing Parquet reader", e);
            }
            currentFileReader = null;
            currentRecordReader = null;
            currentRowGroup = null;
        }
    }

    private List<String> listDataFiles(String location) throws IOException {
        Path path = new Path(location);
        if (!fileSystem.exists(path)) {
            return Collections.emptyList();
        }
        FileStatus[] statuses = fileSystem.listStatus(path);
        List<String> files = new ArrayList<>();
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (!status.isDirectory() && !name.startsWith("_") && !name.startsWith(".")) {
                files.add(status.getPath().toString());
            }
        }
        Collections.sort(files);
        return files;
    }

    private String buildPartitionFilter(String watermark) {
        // For simple single-column partition like dt=2026-04-15,
        // extract the column name and value to build a filter expression
        if (watermark.contains("=")) {
            String[] parts = watermark.split("=", 2);
            return parts[0] + " > \"" + parts[1] + "\"";
        }
        // Fallback: no filter, get all
        return "";
    }

    private String partitionToName(Partition partition) {
        // Reconstruct partition name like "dt=2026-04-15"
        List<String> values = partition.getValues();
        try {
            List<String> keys = metastoreClient.getTable(config.getDatabase(), config.getTable())
                .getPartitionKeys()
                .stream()
                .map(f -> f.getName())
                .collect(Collectors.toList());
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < keys.size(); i++) {
                if (i > 0) sb.append("/");
                sb.append(keys.get(i)).append("=").append(values.get(i));
            }
            return sb.toString();
        } catch (Exception e) {
            return String.join("/", values);
        }
    }

    private byte[] recordToJson(Group record) {
        // Convert Parquet SimpleGroup to JSON
        // SimpleGroup.toString() produces a readable but non-standard format.
        // For PoC, build a simple JSON from the schema fields.
        StringBuilder sb = new StringBuilder("{");
        org.apache.parquet.schema.GroupType schema = record.getType();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (i > 0) sb.append(",");
            String fieldName = schema.getFieldName(i);
            sb.append("\"").append(fieldName).append("\":");
            try {
                String value = record.getValueToString(i, 0);
                // Attempt to detect numeric types
                if (schema.getType(i).isPrimitive()) {
                    switch (schema.getType(i).asPrimitiveType().getPrimitiveTypeName()) {
                        case INT32:
                        case INT64:
                        case FLOAT:
                        case DOUBLE:
                            sb.append(value);
                            break;
                        default:
                            sb.append("\"").append(value.replace("\"", "\\\"")).append("\"");
                    }
                } else {
                    sb.append("\"").append(value.replace("\"", "\\\"")).append("\"");
                }
            } catch (Exception e) {
                sb.append("null");
            }
        }
        sb.append("}");
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return new HivePointer("", "", 0, 0);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        return new HivePointer("", "", 0, sequenceNumber);
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        return new HivePointer("", "", 0, 0);
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return HivePointer.fromString(offset);
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        if (pendingWork == null) return 0;
        int remaining = pendingWork.size() - currentWorkIndex;
        return remaining;
    }

    @Override
    public void close() throws IOException {
        closeCurrentReader();
        if (metastoreTransport != null) {
            metastoreTransport.close();
        }
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    /**
     * Tracks work for a single partition: its files and progress through them.
     */
    private static class PartitionWork {
        final String partitionName;
        final List<String> files;
        int currentFileIndex;

        PartitionWork(String partitionName, List<String> files) {
            this.partitionName = partitionName;
            this.files = files;
            this.currentFileIndex = 0;
        }
    }
}
