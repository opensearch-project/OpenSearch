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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.thrift.transport.TTransportException;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shard consumer that reads from Hive tables via Pull-Based Ingestion.
 * Each shard independently queries Hive Metastore for new partitions (incremental fetch),
 * determines ownership via consistent hashing, and reads assigned Parquet data files.
 */
public class HiveShardConsumer implements IngestionShardConsumer<HivePointer, HiveMessage> {

    private static final Logger logger = LogManager.getLogger(HiveShardConsumer.class);

    private final String clientId;
    private final int shardId;
    private final HiveSourceConfig config;
    private final int numShards;

    // Metastore and filesystem
    private MetastoreCatalog catalog;
    private FileSystem fileSystem;
    private Configuration hadoopConf;
    private MessageType tableSchema;
    private String tableInputFormat;
    private List<String> partitionKeys;

    // Partition tracking
    private String watermark;
    private long watermarkCreateTime;
    private long lastMetastoreQueryTime;

    // Current processing state
    private List<PartitionWork> pendingWork;
    private int currentWorkIndex;
    private HiveFileReader currentFileReader;
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
     * Closes and reconnects to Metastore. Used for recovery after connection failures.
     */
    private void reconnectMetastore() throws IOException {
        catalog.reconnect();
        fetchTableSchema();
    }

    private void fetchTableSchema() throws IOException {
        MetastoreCatalog.TableInfo tableInfo = catalog.getTableInfo(config.getDatabase(), config.getTable());
        tableInputFormat = tableInfo.getInputFormat();
        tableSchema = hiveSchemaToParquet(tableInfo.getColumns());
        partitionKeys = tableInfo.getPartitionKeys();
        logger.info("Table schema for shard {}: {}", shardId, tableSchema);
    }

    private void ensureInitialized() throws Exception {
        if (catalog == null) {
            logger.info("Initializing HiveShardConsumer for shard {} with metastore URI: {}", shardId, config.getMetastoreUri());

            catalog = new ThriftMetastoreCatalog(config);
            catalog.connect();

            AccessController.doPrivilegedChecked(() -> {
                ClassLoader original = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(HiveShardConsumer.class.getClassLoader());
                    hadoopConf = new Configuration();
                    hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
                    fileSystem = FileSystem.get(hadoopConf);
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
            Map<String, Object> row = currentFileReader.readNext();
            if (row == null) {
                closeCurrentReader();
                if (!openNextFile()) {
                    break;
                }
                continue;
            }

            byte[] json = rowToJson(row);
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
        List<MetastoreCatalog.PartitionInfo> partitions;
        if (config.getPartitionOrder() == HiveSourceConfig.PartitionOrder.CREATE_TIME) {
            partitions = discoverByCreateTime();
        } else {
            partitions = discoverByPartitionName();
        }

        logger.info("Shard {} discovered {} candidate partitions from Metastore", shardId, partitions.size());

        // Sort partitions according to ordering strategy
        switch (config.getPartitionOrder()) {
            case CREATE_TIME:
                partitions.sort(Comparator.comparingInt(MetastoreCatalog.PartitionInfo::getCreateTime));
                break;
            case PARTITION_TIME:
                partitions.sort(Comparator.comparing(p -> extractPartitionTime(p)));
                break;
            default:
                partitions.sort((a, b) -> {
                    String aVal = String.join("/", a.getValues());
                    String bVal = String.join("/", b.getValues());
                    return aVal.compareTo(bVal);
                });
        }

        // Filter to partitions assigned to this shard
        for (MetastoreCatalog.PartitionInfo partition : partitions) {
            String partName = partitionToName(partition);
            if (seenPartitions.contains(partName)) continue;
            if (Math.floorMod(partName.hashCode(), numShards) == shardId) {
                List<String> files = listDataFiles(partition.getLocation());
                if (!files.isEmpty()) {
                    pendingWork.add(new PartitionWork(partName, files, partition.getCreateTime()));
                    seenPartitions.add(partName);
                    logger.info("Shard {} assigned partition {} with {} files", shardId, partName, files.size());
                }
            }
        }

        lastMetastoreQueryTime = System.currentTimeMillis();
    }

    private List<MetastoreCatalog.PartitionInfo> discoverByPartitionName() throws IOException {
        if (watermark == null || watermark.isEmpty()) {
            return catalog.getAllPartitions(config.getDatabase(), config.getTable());
        }
        String filter = buildPartitionFilter(watermark);
        return catalog.getPartitionsByFilter(config.getDatabase(), config.getTable(), filter);
    }

    private List<MetastoreCatalog.PartitionInfo> discoverByCreateTime() throws IOException {
        List<MetastoreCatalog.PartitionInfo> all = catalog.getAllPartitions(config.getDatabase(), config.getTable());
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
                currentFileReader = createFileReader(currentFile);
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

    /**
     * Creates a file reader based on the table's input format.
     * Currently supports Parquet. Additional formats (ORC, Avro, etc.) can be added here.
     */
    private HiveFileReader createFileReader(String filePath) throws IOException {
        if (tableInputFormat != null && tableInputFormat.toLowerCase(Locale.ROOT).contains("parquet")) {
            return new ParquetHiveFileReader(filePath, hadoopConf, tableSchema);
        }
        throw new IOException("Unsupported input format: " + tableInputFormat);
    }

    private void closeCurrentReader() {
        if (currentFileReader != null) {
            try {
                currentFileReader.close();
            } catch (IOException e) {
                logger.warn("Error closing file reader", e);
            }
            currentFileReader = null;
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

    private String partitionToName(MetastoreCatalog.PartitionInfo partition) {
        List<String> values = partition.getValues();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (i > 0) sb.append("/");
            sb.append(partitionKeys.get(i)).append("=").append(values.get(i));
        }
        return sb.toString();
    }

    /**
     * Extracts a timestamp string from partition values using the configured partition_time_pattern.
     * Pattern variables like $year, $month, $day are replaced with the corresponding partition key values.
     */
    private String extractPartitionTime(MetastoreCatalog.PartitionInfo partition) {
        String pattern = config.getPartitionTimePattern();
        if (pattern == null) {
            return String.join("/", partition.getValues());
        }
        List<String> values = partition.getValues();
        String result = pattern;
        for (int i = 0; i < partitionKeys.size(); i++) {
            result = result.replace("$" + partitionKeys.get(i), values.get(i));
        }
        return result;
    }

    private byte[] rowToJson(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(entry.getKey()).append("\":");
            Object value = entry.getValue();
            if (value == null) {
                sb.append("null");
            } else if (value instanceof String) {
                sb.append("\"").append(escapeJson((String) value)).append("\"");
            } else if (value instanceof Boolean || value instanceof Number) {
                sb.append(value);
            } else {
                sb.append("\"").append(escapeJson(value.toString())).append("\"");
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
    private static MessageType hiveSchemaToParquet(List<MetastoreCatalog.ColumnInfo> columns) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (MetastoreCatalog.ColumnInfo col : columns) {
            builder.addField(hiveTypeToParquetType(col.getName(), col.getType()));
        }
        return builder.named("table");
    }

    private static Type hiveTypeToParquetType(String name, String hiveType) {
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
        if (catalog != null) {
            catalog.close();
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
