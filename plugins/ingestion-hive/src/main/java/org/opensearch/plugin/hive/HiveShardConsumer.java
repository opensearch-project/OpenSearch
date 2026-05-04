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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.plugin.hive.metastore.FieldSchema;
import org.opensearch.plugin.hive.metastore.GetTableRequest;
import org.opensearch.plugin.hive.metastore.GetTableResult;
import org.opensearch.plugin.hive.metastore.Partition;
import org.opensearch.plugin.hive.metastore.Table;
import org.opensearch.plugin.hive.metastore.ThriftHiveMetastore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shard consumer that reads from Hive tables via Pull-Based Ingestion.
 * Each shard independently queries Hive Metastore for new partitions (incremental fetch),
 * determines ownership via consistent hashing, and reads assigned Parquet data files.
 * This follows the same pattern as Flink's ContinuousPartitionFetcher.
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
    private MessageType tableSchema;
    private List<String> partitionKeys;

    // Partition tracking
    private String watermark;
    private long watermarkCreateTime;
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
    private final Set<String> seenPartitions = new HashSet<>();

    /**
     * Creates a new HiveShardConsumer.
     *
     * @param clientId the client identifier for this consumer
     * @param shardId the shard ID this consumer is responsible for
     * @param config the Hive source configuration
     */
    public HiveShardConsumer(String clientId, int shardId, HiveSourceConfig config) {
        this.clientId = clientId;
        this.shardId = shardId;
        this.config = config;
        this.numShards = config.getNumShards();
        this.watermark = config.getConsumeStartOffset();
        this.watermarkCreateTime = 0;
        this.pendingWork = new ArrayList<>();
        this.currentWorkIndex = 0;
        this.sequenceNumber = 0;
        this.lastMetastoreQueryTime = 0;
    }

    /**
     * Connects to Hive Metastore with retry logic.
     * Supports both framed (Hive 3) and unframed (Hive 4) transport modes.
     */
    private void connectToMetastore() throws TTransportException {
        String uri = config.getMetastoreUri().replace("thrift://", "");
        String[] hostPort = uri.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        TTransportException lastException = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                if (attempt > 0) {
                    logger.info("Retrying Metastore connection for shard {} (attempt {}/{})", shardId, attempt, config.getMaxRetries());
                    Thread.sleep(config.getRetryIntervalMillis());
                }

                TSocket socket = new TSocket(host, port, config.getConnectTimeoutMillis());
                TTransport transport;
                if (config.getTransportMode() == HiveSourceConfig.TransportMode.FRAMED) {
                    transport = new TFramedTransport(socket);
                } else {
                    transport = socket;
                }
                transport.open();

                metastoreTransport = transport;
                metastoreClient = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
                logger.info(
                    "Connected to Hive Metastore for shard {} at {}:{} (transport={})",
                    shardId,
                    host,
                    port,
                    config.getTransportMode()
                );
                return;
            } catch (TTransportException e) {
                lastException = e;
                logger.warn("Failed to connect to Metastore for shard {} (attempt {}): {}", shardId, attempt + 1, e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TTransportException("Interrupted during retry", e);
            }
        }
        throw lastException;
    }

    /**
     * Closes and reconnects to Metastore. Used for recovery after connection failures.
     */
    private void reconnectMetastore() throws TTransportException {
        closeMetastore();
        connectToMetastore();
        // Re-fetch table schema after reconnect
        try {
            fetchTableSchema();
        } catch (TException e) {
            throw new TTransportException("Failed to fetch table schema after reconnect", e);
        }
    }

    private void closeMetastore() {
        if (metastoreTransport != null) {
            try {
                metastoreTransport.close();
            } catch (Exception e) {
                logger.debug("Error closing metastore transport", e);
            }
            metastoreTransport = null;
            metastoreClient = null;
        }
    }

    private void fetchTableSchema() throws TException {
        GetTableRequest req = new GetTableRequest();
        req.setDbName(config.getDatabase());
        req.setTblName(config.getTable());
        GetTableResult result = metastoreClient.get_table_req(req);
        Table table = result.getTable();
        tableSchema = hiveSchemaToParquet(table.getSd().getCols());
        partitionKeys = table.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
        logger.info("Table schema for shard {}: {}", shardId, tableSchema);
    }

    private void ensureInitialized() throws Exception {
        if (metastoreClient == null) {
            logger.info("Initializing HiveShardConsumer for shard {} with metastore URI: {}", shardId, config.getMetastoreUri());

            connectToMetastore();

            hadoopConf = java.security.AccessController.doPrivileged((java.security.PrivilegedExceptionAction<Configuration>) () -> {
                ClassLoader original = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(HiveShardConsumer.class.getClassLoader());
                    Configuration conf = new Configuration();
                    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
                    return conf;
                } finally {
                    Thread.currentThread().setContextClassLoader(original);
                }
            });
            fileSystem = java.security.AccessController.doPrivileged((java.security.PrivilegedExceptionAction<FileSystem>) () -> {
                ClassLoader original = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(HiveShardConsumer.class.getClassLoader());
                    return FileSystem.get(hadoopConf);
                } finally {
                    Thread.currentThread().setContextClassLoader(original);
                }
            });

            fetchTableSchema();
            logger.info("HiveShardConsumer initialized successfully for shard {}", shardId);
        }
    }

    @Override
    public List<ReadResult<HivePointer, HiveMessage>> readNext(
        HivePointer pointer,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) {
        try {
            return doReadNext(maxMessages);
        } catch (TTransportException e) {
            logger.warn("Metastore connection lost for shard {}, attempting reconnect: {}", shardId, e.getMessage());
            try {
                reconnectMetastore();
                return doReadNext(maxMessages);
            } catch (Exception retryEx) {
                logger.error("Failed to recover Metastore connection for shard {}: {}", shardId, retryEx.getMessage());
                return Collections.emptyList();
            }
        } catch (Throwable e) {
            logger.error("Error reading from Hive table for shard {}: {}", shardId, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public List<ReadResult<HivePointer, HiveMessage>> readNext(long maxMessages, int timeoutMillis) {
        try {
            return doReadNext(maxMessages);
        } catch (TTransportException e) {
            logger.warn("Metastore connection lost for shard {}, attempting reconnect: {}", shardId, e.getMessage());
            try {
                reconnectMetastore();
                return doReadNext(maxMessages);
            } catch (Exception retryEx) {
                logger.error("Failed to recover Metastore connection for shard {}: {}", shardId, retryEx.getMessage());
                return Collections.emptyList();
            }
        } catch (Throwable e) {
            logger.error("Error reading from Hive table for shard {}: {}", shardId, e.getMessage());
            return Collections.emptyList();
        }
    }

    private List<ReadResult<HivePointer, HiveMessage>> doReadNext(long maxMessages) throws Exception {
        ensureInitialized();

        if (shouldRefreshPartitions()) {
            discoverNewPartitions();
        }

        if (currentWorkIndex >= pendingWork.size() && currentFileReader == null) {
            return Collections.emptyList();
        }

        if (currentFileReader == null) {
            if (!openNextFile()) {
                return Collections.emptyList();
            }
        }

        List<ReadResult<HivePointer, HiveMessage>> results = new ArrayList<>();
        long count = 0;
        while (count < maxMessages) {
            if (currentRecordReader == null || currentRowGroupRowsRemaining <= 0) {
                currentRowGroup = currentFileReader.readNextRowGroup();
                if (currentRowGroup == null) {
                    closeCurrentReader();
                    if (!openNextFile()) {
                        break;
                    }
                    continue;
                }
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(tableSchema, currentSchema);
                currentRecordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(tableSchema));
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
     * Supports both partition-name and create-time ordering strategies.
     */
    private void discoverNewPartitions() throws Exception {
        List<Partition> partitions;
        if (config.getPartitionOrder() == HiveSourceConfig.PartitionOrder.CREATE_TIME) {
            partitions = discoverByCreateTime();
        } else {
            partitions = discoverByPartitionName();
        }

        logger.info("Shard {} discovered {} candidate partitions from Metastore", shardId, partitions.size());

        // Sort partitions according to ordering strategy
        if (config.getPartitionOrder() == HiveSourceConfig.PartitionOrder.CREATE_TIME) {
            partitions.sort(Comparator.comparingInt(Partition::getCreateTime));
        } else {
            partitions.sort((a, b) -> {
                String aVal = String.join("/", a.getValues());
                String bVal = String.join("/", b.getValues());
                return aVal.compareTo(bVal);
            });
        }

        // Filter to partitions assigned to this shard
        for (Partition partition : partitions) {
            String partName = partitionToName(partition);
            if (seenPartitions.contains(partName)) continue;
            if (Math.floorMod(partName.hashCode(), numShards) == shardId) {
                List<String> files = listDataFiles(partition.getSd().getLocation());
                if (!files.isEmpty()) {
                    pendingWork.add(new PartitionWork(partName, files, partition.getCreateTime()));
                    seenPartitions.add(partName);
                    logger.info("Shard {} assigned partition {} with {} files", shardId, partName, files.size());
                }
            }
        }

        lastMetastoreQueryTime = System.currentTimeMillis();
    }

    private List<Partition> discoverByPartitionName() throws TException {
        if (watermark == null || watermark.isEmpty()) {
            return metastoreClient.get_partitions(config.getDatabase(), config.getTable(), (short) -1);
        }
        String filter = buildPartitionFilter(watermark);
        return metastoreClient.get_partitions_by_filter(config.getDatabase(), config.getTable(), filter, (short) -1);
    }

    private List<Partition> discoverByCreateTime() throws TException {
        // create-time mode: get all partitions and filter by createTime > watermarkCreateTime
        List<Partition> all = metastoreClient.get_partitions(config.getDatabase(), config.getTable(), (short) -1);
        if (watermarkCreateTime <= 0 && (watermark == null || watermark.isEmpty())) {
            return all;
        }
        return all.stream().filter(p -> p.getCreateTime() > watermarkCreateTime).collect(Collectors.toList());
    }

    private boolean shouldRefreshPartitions() {
        if (lastMetastoreQueryTime == 0) return true;
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
                watermarkCreateTime = work.createTime;
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
        if (watermark.contains("=")) {
            String[] parts = watermark.split("=", 2);
            return parts[0] + " > \"" + parts[1] + "\"";
        }
        return "";
    }

    private String partitionToName(Partition partition) {
        List<String> values = partition.getValues();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (i > 0) sb.append("/");
            sb.append(partitionKeys.get(i)).append("=").append(values.get(i));
        }
        return sb.toString();
    }

    private byte[] recordToJson(Group record) {
        StringBuilder sb = new StringBuilder("{");
        org.apache.parquet.schema.GroupType schema = record.getType();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (i > 0) sb.append(",");
            String fieldName = schema.getFieldName(i);
            sb.append("\"").append(fieldName).append("\":");
            int repetitionCount = record.getFieldRepetitionCount(i);
            if (repetitionCount == 0) {
                sb.append("null");
                continue;
            }
            if (schema.getType(i).isPrimitive()) {
                switch (schema.getType(i).asPrimitiveType().getPrimitiveTypeName()) {
                    case BOOLEAN:
                        sb.append(record.getBoolean(i, 0));
                        break;
                    case INT32:
                        sb.append(record.getInteger(i, 0));
                        break;
                    case INT64:
                        sb.append(record.getLong(i, 0));
                        break;
                    case FLOAT:
                        sb.append(record.getFloat(i, 0));
                        break;
                    case DOUBLE:
                        sb.append(record.getDouble(i, 0));
                        break;
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        String strVal = record.getString(i, 0);
                        sb.append("\"").append(escapeJson(strVal)).append("\"");
                        break;
                    case INT96:
                        sb.append("\"").append(record.getValueToString(i, 0)).append("\"");
                        break;
                    default:
                        sb.append("\"").append(escapeJson(record.getValueToString(i, 0))).append("\"");
                }
            } else {
                sb.append("\"").append(escapeJson(record.getValueToString(i, 0))).append("\"");
            }
        }
        sb.append("}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
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

    /**
     * Converts Hive FieldSchema list to Parquet MessageType.
     * Used as the projection schema so that files with fewer columns
     * return null for missing fields.
     */
    private static MessageType hiveSchemaToParquet(List<FieldSchema> columns) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (FieldSchema col : columns) {
            builder.addField(hiveTypeToParquetType(col.getName(), col.getType()));
        }
        return builder.named("table");
    }

    private static org.apache.parquet.schema.Type hiveTypeToParquetType(String name, String hiveType) {
        switch (hiveType.toLowerCase(Locale.ROOT)) {
            case "boolean":
                return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
            case "tinyint":
            case "smallint":
            case "int":
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);
            case "bigint":
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
            case "float":
                return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);
            case "double":
                return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
            case "string":
            case "varchar":
            case "char":
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
            case "binary":
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
            case "timestamp":
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT96).named(name);
            case "date":
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(name);
            case "decimal":
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.decimalType(0, 10)).named(name);
            default:
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
        }
    }

    @Override
    public void close() throws IOException {
        closeCurrentReader();
        closeMetastore();
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
        final int createTime;
        int currentFileIndex;

        PartitionWork(String partitionName, List<String> files, int createTime) {
            this.partitionName = partitionName;
            this.files = files;
            this.createTime = createTime;
            this.currentFileIndex = 0;
        }
    }
}
