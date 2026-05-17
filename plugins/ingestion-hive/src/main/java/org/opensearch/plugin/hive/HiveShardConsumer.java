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
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shard consumer that reads from Hive tables via Pull-Based Ingestion.
 * Each shard independently queries Hive Metastore for new partitions (incremental fetch),
 * determines ownership via consistent hashing, and reads assigned Parquet data files.
 */
public class HiveShardConsumer implements IngestionShardConsumer<HivePointer, HiveMessage> {

    private static final Logger logger = LogManager.getLogger(HiveShardConsumer.class);

    private final int shardId;
    private final HiveSourceConfig config;
    private final int numShards;

    // Catalog connection for metadata queries (table schema, partition discovery)
    private MetastoreCatalog catalog;
    private Configuration hadoopConf;
    private MessageType tableSchema;
    private String tableInputFormat;
    List<String> partitionKeys;

    // Partition tracking: watermark tracks the last fully-processed partition so that
    // incremental discovery only fetches partitions newer than the watermark.
    private String watermark;
    private String watermarkPartitionTime;
    private long watermarkCreateTime;
    private long lastMetastoreQueryTime;

    // Current processing state: pendingWork is the queue of partitions to process.
    // Each PartitionWork tracks its files and progress. sequenceNumber is a monotonically
    // increasing counter used for pointer ordering and checkpoint recovery.
    private List<PartitionWork> pendingWork;
    private int currentWorkIndex;
    private HiveFileReader currentFileReader;
    private String currentFile;
    private long currentRowIndex;
    private long sequenceNumber;
    private boolean resumed;
    private boolean seekInclusive;

    /**
     * Creates a new HiveShardConsumer.
     *
     * @param clientId the client identifier for this consumer
     * @param shardId the shard ID this consumer is responsible for
     * @param config the Hive source configuration
     */
    public HiveShardConsumer(String clientId, int shardId, HiveSourceConfig config) {
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

    /**
     * Seeks to the position indicated by the pointer. Sets the watermark to the pointer's partition
     * so that partition discovery resumes from that point, then seeks within the partition to the
     * correct file and row.
     */
    private void seekToPointer(HivePointer pointer, boolean includeStart) throws Exception {
        // Handle "latest" reset: skip all existing partitions, only read new ones
        if ("__LATEST__".equals(pointer.getPartitionName())) {
            List<MetastoreCatalog.PartitionInfo> existing = catalog.getAllPartitions(config.getDatabase(), config.getTable());
            if (!existing.isEmpty()) {
                watermark = existing.stream().map(this::partitionToName).max(String::compareTo).orElse("");
                watermarkPartitionTime = existing.stream().map(this::extractPartitionTime).max(String::compareTo).orElse("");
                watermarkCreateTime = existing.stream().mapToInt(MetastoreCatalog.PartitionInfo::getCreateTime).max().orElse(0);
            }
            sequenceNumber = 0;
            logger.info("Shard {} reset to latest, skipping {} existing partitions", shardId, existing.size());
            return;
        }

        // Set watermark to the pointer's partition and use inclusive filter so that
        // discoverNewPartitions fetches this partition and everything after it.
        this.watermark = pointer.getPartitionName();
        this.watermarkCreateTime = 0;
        this.sequenceNumber = includeStart ? pointer.getSequenceNumber() : pointer.getSequenceNumber() + 1;
        this.pendingWork = new ArrayList<>();
        this.currentWorkIndex = 0;
        this.seekInclusive = true;

        // Discover partitions from the pointer's partition onward (inclusive)
        discoverNewPartitions();
        this.seekInclusive = false;

        // Seek to the correct file within the pointer's partition
        String targetFile = pointer.getFilePath();
        long targetRow = includeStart ? pointer.getRowIndex() : pointer.getRowIndex() + 1;

        for (int i = 0; i < pendingWork.size(); i++) {
            PartitionWork work = pendingWork.get(i);
            if (work.partitionName.equals(pointer.getPartitionName())) {
                // Set watermark to the partition before the target so incremental
                // discovery will include the target partition's successors
                watermark = pointer.getPartitionName();
                watermarkPartitionTime = work.partitionTime;
                watermarkCreateTime = work.createTime;
                currentWorkIndex = i;
                for (int f = 0; f < work.files.size(); f++) {
                    if (work.files.get(f).equals(targetFile)) {
                        work.currentFileIndex = f + 1;
                        currentFile = work.files.get(f);
                        currentFileReader = createFileReader(currentFile);
                        // Skip rows up to target position
                        for (long r = 0; r < targetRow; r++) {
                            if (currentFileReader.readNext() == null) break;
                        }
                        currentRowIndex = targetRow;
                        logger.info(
                            "Shard {} seeked to partition={}, file={}, row={}",
                            shardId,
                            pointer.getPartitionName(),
                            targetFile,
                            targetRow
                        );
                        return;
                    }
                }
                break;
            }
        }
        // Partition/file not found, set watermark to pointer's partition so next discovery skips past it
        watermark = pointer.getPartitionName();
        logger.info("Shard {} seek target not found, continuing from watermark {}", shardId, watermark);
    }

    private void fetchTableSchema() throws IOException {
        MetastoreCatalog.TableInfo tableInfo = catalog.getTableInfo(config.getDatabase(), config.getTable());
        tableInputFormat = tableInfo.getInputFormat();
        tableSchema = hiveSchemaToParquet(tableInfo.getColumns());
        partitionKeys = tableInfo.getPartitionKeys();
        if (partitionKeys == null) {
            partitionKeys = Collections.emptyList();
        }
        logger.info("Table schema for shard {}: {}, partitionKeys: {}", shardId, tableSchema, partitionKeys);
    }

    private void ensureInitialized() throws Exception {
        if (catalog == null) {
            logger.info("Initializing HiveShardConsumer for shard {} with metastore URI: {}", shardId, config.getMetastoreUri());

            MetastoreCatalog newCatalog = new ThriftMetastoreCatalog(config);
            newCatalog.connect();

            try {
                AccessController.doPrivilegedChecked(() -> {
                    ClassLoader original = Thread.currentThread().getContextClassLoader();
                    try {
                        Thread.currentThread().setContextClassLoader(HiveShardConsumer.class.getClassLoader());
                        hadoopConf = new Configuration();
                        String s3FsImpl = S3HadoopFileSystem.class.getName();
                        hadoopConf.set("fs.s3.impl", s3FsImpl);
                        hadoopConf.set("fs.s3a.impl", s3FsImpl);
                        hadoopConf.set("fs.s3n.impl", s3FsImpl);
                        for (Map.Entry<String, String> entry : config.getHadoopProperties().entrySet()) {
                            hadoopConf.set(entry.getKey(), entry.getValue());
                        }
                    } finally {
                        Thread.currentThread().setContextClassLoader(original);
                    }
                });
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to initialize Hadoop config for shard {}", shardId), e);
                newCatalog.close();
                throw e;
            }

            catalog = newCatalog;

            try {
                fetchTableSchema();
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to fetch table schema for shard {}", shardId), e);
                catalog.close();
                catalog = null;
                hadoopConf = null;
                throw e;
            }
            logger.info("HiveShardConsumer initialized successfully for shard {}", shardId);
        }
    }

    /**
     * Resumes reading from the given pointer position. Restores watermark, seeks to the
     * correct partition and file, and skips rows up to the pointer's row index.
     */
    @Override
    public List<ReadResult<HivePointer, HiveMessage>> readNext(
        HivePointer pointer,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) {
        return executeWithRetry(() -> {
            ensureInitialized();
            if (!resumed && pointer != null && !pointer.getPartitionName().isEmpty()) {
                seekToPointer(pointer, includeStart);
                resumed = true;
            }
            return doReadNext(maxMessages);
        });
    }

    @Override
    public List<ReadResult<HivePointer, HiveMessage>> readNext(long maxMessages, int timeoutMillis) {
        return executeWithRetry(() -> doReadNext(maxMessages));
    }

    private List<ReadResult<HivePointer, HiveMessage>> executeWithRetry(ReadAction action) {
        try {
            return action.execute();
        } catch (TTransportException e) {
            logger.warn("Metastore connection lost for shard {}, attempting reconnect: {}", shardId, e.getMessage());
            try {
                reconnectMetastore();
                return action.execute();
            } catch (Exception retryEx) {
                logger.error("Failed to recover Metastore connection for shard {}: {}", shardId, retryEx.getMessage());
                return Collections.emptyList();
            }
        } catch (Throwable e) {
            logger.error("Error reading from Hive table for shard {}: {}", shardId, e.getMessage());
            return Collections.emptyList();
        }
    }

    @FunctionalInterface
    private interface ReadAction {
        List<ReadResult<HivePointer, HiveMessage>> execute() throws Exception;
    }

    /**
     * Core read loop. Discovers new partitions if needed, then reads rows from the current
     * file and converts them to JSON messages. Returns up to maxMessages results per call.
     */
    private List<ReadResult<HivePointer, HiveMessage>> doReadNext(long maxMessages) throws Exception {
        ensureInitialized();

        // Step 1: Check if it's time to query Metastore for new partitions.
        // This only triggers when all current work is done and the monitor interval has elapsed.
        if (shouldRefreshPartitions()) {
            discoverNewPartitions();
        }

        // Step 2: If there's nothing to read (no pending partitions, no open file), return empty.
        if (currentWorkIndex >= pendingWork.size() && currentFileReader == null) {
            return Collections.emptyList();
        }

        // Step 3: If no file is currently open, open the next one from pending work.
        if (currentFileReader == null) {
            if (!openNextFile()) {
                return Collections.emptyList();
            }
        }

        // Step 4: Read rows from the current file, converting each to a JSON message.
        // When a file is exhausted, move to the next file (possibly in the next partition).
        // Stop when maxMessages is reached or all pending files are consumed.
        List<ReadResult<HivePointer, HiveMessage>> results = new ArrayList<>();
        long count = 0;
        while (count < maxMessages) {
            Map<String, Object> row = currentFileReader.readNext();
            if (row == null) {
                // Current file exhausted, try the next file
                closeCurrentReader();
                if (!openNextFile()) {
                    break;
                }
                continue;
            }

            PartitionWork work = pendingWork.get(currentWorkIndex);
            HivePointer ptr = new HivePointer(work.partitionName, currentFile, currentRowIndex, sequenceNumber);
            byte[] json = rowToJson(row, ptr);
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
        switch (config.getPartitionOrder()) {
            case CREATE_TIME:
                partitions = discoverByCreateTime();
                break;
            case PARTITION_TIME:
                // Partition-time ordering cannot use Metastore server-side filter because
                // lexicographic order may differ from extracted time order (e.g., hour=2 vs hour=11).
                // Full retrieval with client-side filtering by extracted time is required.
                partitions = discoverByPartitionTime();
                break;
            default:
                partitions = discoverByPartitionName();
                break;
        }

        logger.info("Shard {} discovered {} candidate partitions from Metastore", shardId, partitions.size());

        // Sort partitions according to ordering strategy
        switch (config.getPartitionOrder()) {
            case CREATE_TIME:
                partitions.sort(Comparator.comparingInt(MetastoreCatalog.PartitionInfo::getCreateTime));
                break;
            case PARTITION_TIME:
                partitions.sort(Comparator.comparing(this::extractPartitionTime));
                break;
            default:
                partitions.sort((a, b) -> {
                    String aVal = String.join("/", a.getValues());
                    String bVal = String.join("/", b.getValues());
                    return aVal.compareTo(bVal);
                });
        }

        // Assign partitions to this shard using consistent hashing on partition name.
        // Each shard deterministically owns a subset of partitions (no coordination needed).
        for (MetastoreCatalog.PartitionInfo partition : partitions) {
            String partName = partitionToName(partition);
            if (Math.floorMod(partName.hashCode(), numShards) == shardId) {
                List<String> files = listDataFiles(partition.getLocation());
                if (!files.isEmpty()) {
                    String partTime = extractPartitionTime(partition);
                    pendingWork.add(new PartitionWork(partName, partTime, files, partition.getCreateTime()));
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
        String filter = buildPartitionFilter(watermark, seekInclusive);
        return catalog.getPartitionsByFilter(config.getDatabase(), config.getTable(), filter);
    }

    // Partition-time mode: full retrieval with client-side filtering by extracted timestamp.
    // Cannot use Metastore filter because lexicographic order may differ from time order.
    private List<MetastoreCatalog.PartitionInfo> discoverByPartitionTime() throws IOException {
        List<MetastoreCatalog.PartitionInfo> all = catalog.getAllPartitions(config.getDatabase(), config.getTable());
        if (watermarkPartitionTime == null || watermarkPartitionTime.isEmpty()) {
            return all;
        }
        return all.stream().filter(p -> extractPartitionTime(p).compareTo(watermarkPartitionTime) > 0).collect(Collectors.toList());
    }

    // Neither Hive Metastore nor AWS Glue support server-side filtering by createTime.
    // Client-side filtering after full partition list retrieval is the only option.
    private List<MetastoreCatalog.PartitionInfo> discoverByCreateTime() throws IOException {
        List<MetastoreCatalog.PartitionInfo> all = catalog.getAllPartitions(config.getDatabase(), config.getTable());
        if (watermarkCreateTime <= 0 && (watermark == null || watermark.isEmpty())) {
            return all;
        }
        return all.stream().filter(p -> p.getCreateTime() > watermarkCreateTime).collect(Collectors.toList());
    }

    /**
     * Determines if it's time to query the Metastore for new partitions.
     * Only refreshes when all pending work is complete and the monitor interval has elapsed,
     * preventing excessive Metastore queries during active reading.
     */
    private boolean shouldRefreshPartitions() {
        if (lastMetastoreQueryTime == 0) return true;
        boolean noPendingWork = currentWorkIndex >= pendingWork.size() && currentFileReader == null;
        boolean intervalElapsed = System.currentTimeMillis() - lastMetastoreQueryTime >= config.getMonitorIntervalMillis();
        return noPendingWork && intervalElapsed;
    }

    /**
     * Opens the next data file from the pending work queue. When all files in a partition
     * are consumed, updates the watermark and advances to the next partition.
     */
    private boolean openNextFile() throws IOException {
        while (currentWorkIndex < pendingWork.size()) {
            PartitionWork work = pendingWork.get(currentWorkIndex);
            if (work.currentFileIndex < work.files.size()) {
                // More files in current partition: open the next one
                currentFile = work.files.get(work.currentFileIndex);
                work.currentFileIndex++;
                currentRowIndex = 0;
                currentFileReader = createFileReader(currentFile);
                return true;
            } else {
                // All files in this partition are consumed. Advance watermark so that
                // future partition discovery skips this partition, then move to the next.
                watermark = work.partitionName;
                watermarkPartitionTime = work.partitionTime;
                watermarkCreateTime = work.createTime;
                currentWorkIndex++;
            }
        }
        // All pending partitions have been fully read
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
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HiveShardConsumer.class.getClassLoader());
            FileSystem fs = FileSystem.get(path.toUri(), hadoopConf);
            if (!fs.exists(path)) {
                return Collections.emptyList();
            }
            FileStatus[] statuses = fs.listStatus(path);
            List<String> files = new ArrayList<>();
            for (FileStatus status : statuses) {
                String name = status.getPath().getName();
                if (!status.isDirectory() && !name.startsWith("_") && !name.startsWith(".")) {
                    files.add(status.getPath().toString());
                }
            }
            Collections.sort(files);
            return files;
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    /**
     * Builds a Metastore partition filter expression that matches partitions lexicographically
     * after the watermark. For composite partitions (e.g., "year=2024/month=01/day=15"),
     * generates: (year > "2024") OR (year = "2024" AND month > "01") OR
     *            (year = "2024" AND month = "01" AND day > "15")
     * When inclusive is true, uses >= for the last key to include the watermark partition itself.
     */
    String buildPartitionFilter(String watermark, boolean inclusive) {
        String[] segments = watermark.split("/");
        List<String> clauses = new ArrayList<>();
        for (int i = 0; i < segments.length; i++) {
            if (!segments[i].contains("=")) continue;
            StringBuilder clause = new StringBuilder();
            for (int j = 0; j < i; j++) {
                String[] kv = segments[j].split("=", 2);
                if (!clause.isEmpty()) clause.append(" AND ");
                clause.append(kv[0]).append(" = \"").append(kv[1]).append("\"");
            }
            String[] kv = segments[i].split("=", 2);
            if (!clause.isEmpty()) clause.append(" AND ");
            String op = (inclusive && i == segments.length - 1) ? " >= " : " > ";
            clause.append(kv[0]).append(op).append("\"").append(kv[1]).append("\"");
            clauses.add("(" + clause + ")");
        }
        return clauses.isEmpty() ? "" : String.join(" OR ", clauses);
    }

    String partitionToName(MetastoreCatalog.PartitionInfo partition) {
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
    String extractPartitionTime(MetastoreCatalog.PartitionInfo partition) {
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

    byte[] rowToJson(Map<String, Object> row, HivePointer pointer) {
        StringBuilder sb = new StringBuilder("{\"_id\":\"");
        sb.append(escapeJson(pointer.asString()));
        sb.append("\",\"_source\":{");
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
        sb.append("}}");
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
        // Signals that existing partitions should be skipped. seekToPointer sets all watermarks
        // to their respective maximums so that subsequent discovery returns no existing partitions.
        return new HivePointer("__LATEST__", "", 0, Long.MAX_VALUE - 1);
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        throw new UnsupportedOperationException(
            "Hive ingestion does not support timestamp-based pointer reset. Use 'earliest' or 'latest' instead."
        );
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
        return pendingWork.size() - currentWorkIndex;
    }

    /**
     * Converts Hive FieldSchema list to Parquet MessageType.
     * Used as the projection schema so that files with fewer columns
     * return null for missing fields.
     */
    static MessageType hiveSchemaToParquet(List<MetastoreCatalog.ColumnInfo> columns) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (MetastoreCatalog.ColumnInfo col : columns) {
            builder.addField(hiveTypeToParquetType(col.getName(), col.getType()));
        }
        return builder.named("table");
    }

    private static Type hiveTypeToParquetType(String name, String hiveType) {
        return switch (hiveType.toLowerCase(Locale.ROOT)) {
            case "boolean" -> Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
            case "tinyint", "smallint", "int" -> Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);
            case "bigint" -> Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
            case "float" -> Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);
            case "double" -> Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
            case "string", "varchar", "char" -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(name);
            case "binary" -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
            case "timestamp" -> Types.optional(PrimitiveType.PrimitiveTypeName.INT96).named(name);
            case "date" -> Types.optional(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(name);
            case "decimal" -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.decimalType(0, 10))
                .named(name);
            default -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
        };
    }

    @Override
    public void close() throws IOException {
        closeCurrentReader();
        if (catalog != null) {
            catalog.close();
        }
    }

    /**
     * Tracks work for a single partition: its files and progress through them.
     */
    private static class PartitionWork {
        final String partitionName;
        final String partitionTime;
        final List<String> files;
        final int createTime;
        int currentFileIndex;

        PartitionWork(String partitionName, String partitionTime, List<String> files, int createTime) {
            this.partitionName = partitionName;
            this.partitionTime = partitionTime;
            this.files = files;
            this.createTime = createTime;
            this.currentFileIndex = 0;
        }
    }
}
