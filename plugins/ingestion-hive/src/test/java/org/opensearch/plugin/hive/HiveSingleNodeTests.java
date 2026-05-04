/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.hive.metastore.Database;
import org.opensearch.plugin.hive.metastore.FieldSchema;
import org.opensearch.plugin.hive.metastore.Partition;
import org.opensearch.plugin.hive.metastore.SerDeInfo;
import org.opensearch.plugin.hive.metastore.StorageDescriptor;
import org.opensearch.plugin.hive.metastore.Table;
import org.opensearch.plugin.hive.metastore.ThriftHiveMetastore;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.client.Requests;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Integration test for Hive ingestion using Testcontainers with Hive Metastore.
 */
@SuppressForbidden(reason = "Parquet and Hadoop APIs require java.io.File")
@ThreadLeakFilters(filters = HiveSingleNodeTests.TestContainerThreadLeakFilter.class)
public class HiveSingleNodeTests extends OpenSearchSingleNodeTestCase {

    private static final String DATABASE = "test_db";
    private static final String TABLE_NAME = "events";
    private static final String WAREHOUSE_CONTAINER_PATH = "/opt/hive/data/warehouse";

    private static final MessageType PARQUET_SCHEMA = MessageTypeParser.parseMessageType(
        "message events {\n"
            + "  optional binary event_id (UTF8);\n"
            + "  optional binary user_id (UTF8);\n"
            + "  optional binary event_type (UTF8);\n"
            + "  optional int64 timestamp;\n"
            + "  optional double amount;\n"
            + "}"
    );

    private GenericContainer<?> hiveMetastore;
    private File warehouseDir;
    private String metastoreUri;
    private final String indexName = "hive-test-index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(HiveIngestionPlugin.class);
    }

    @Before
    public void setup() throws Exception {
        Assume.assumeTrue("Docker is not available", DockerClientFactory.instance().isDockerAvailable());
        warehouseDir = createTempDir().toFile();
        generateTestData();
        setupHiveMetastore();
        registerTableAndPartitions();
        metastoreUri = "thrift://" + hiveMetastore.getHost() + ":" + hiveMetastore.getMappedPort(9083);
    }

    @After
    public void cleanup() {
        if (hiveMetastore != null) {
            hiveMetastore.stop();
        }
        deleteDirectory(warehouseDir);
    }

    /**
     * Test basic ingestion: 3 partitions x 150 rows = 450 documents.
     */
    public void testBasicIngestion() throws Exception {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "HIVE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.metastore_uri", metastoreUri)
                .put("ingestion_source.param.database", DATABASE)
                .put("ingestion_source.param.table", TABLE_NAME)
                .put("ingestion_source.param.monitor_interval", "2s")
                .put("index.replication.type", "SEGMENT")
                .build()
        );
        ensureGreen(indexName);

        waitForState(() -> {
            SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            return response.getHits().getTotalHits().value() == 450;
        });
    }

    /**
     * Test recovery after pause/resume: ingestion resumes from the last committed pointer
     * and new partitions added during pause are picked up after resume.
     */
    public void testPauseResumeRecovery() throws Exception {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "HIVE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.metastore_uri", metastoreUri)
                .put("ingestion_source.param.database", DATABASE)
                .put("ingestion_source.param.table", TABLE_NAME)
                .put("ingestion_source.param.monitor_interval", "2s")
                .put("index.replication.type", "SEGMENT")
                .build()
        );
        ensureGreen(indexName);

        // Wait for initial 450 documents
        waitForState(() -> {
            SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            return response.getHits().getTotalHits().value() == 450;
        });

        // Pause ingestion
        client().admin().indices().pauseIngestion(Requests.pauseIngestionRequest(indexName)).get();

        // Add a new partition while paused
        addPartition("2026-04-16", 100);

        // Resume ingestion
        client().admin().indices().resumeIngestion(Requests.resumeIngestionRequest(indexName)).get();

        // Wait for new partition to be ingested (450 + 100 = 550)
        waitForState(() -> {
            SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            return response.getHits().getTotalHits().value() == 550;
        });
    }

    /**
     * Add a new partition with test data to the Hive table.
     */
    private void addPartition(String dtValue, int numRows) throws Exception {
        // Write Parquet file
        File partDir = new File(warehouseDir, DATABASE + "/" + TABLE_NAME + "/dt=" + dtValue);
        partDir.mkdirs();
        writeParquetFile(new File(partDir, "part-00000.parquet"), numRows, 1000);

        // Register partition in Metastore
        TTransport transport = new TSocket(hiveMetastore.getHost(), hiveMetastore.getMappedPort(9083), 10000);
        transport.open();
        ThriftHiveMetastore.Client client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
        try {
            List<FieldSchema> cols = new ArrayList<>();
            cols.add(new FieldSchema("event_id", "string", ""));
            cols.add(new FieldSchema("user_id", "string", ""));
            cols.add(new FieldSchema("event_type", "string", ""));
            cols.add(new FieldSchema("timestamp", "bigint", ""));
            cols.add(new FieldSchema("amount", "double", ""));

            SerDeInfo serDeInfo = new SerDeInfo();
            serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");

            StorageDescriptor partSd = new StorageDescriptor();
            partSd.setCols(cols);
            partSd.setLocation(WAREHOUSE_CONTAINER_PATH + "/" + DATABASE + "/" + TABLE_NAME + "/dt=" + dtValue);
            partSd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
            partSd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
            partSd.setSerdeInfo(serDeInfo);

            Partition partition = new Partition();
            partition.setValues(List.of(dtValue));
            partition.setDbName(DATABASE);
            partition.setTableName(TABLE_NAME);
            partition.setSd(partSd);

            client.add_partition(partition);
        } finally {
            transport.close();
        }
    }

    /**
     * Generate Parquet test data: 3 partitions, 2 files each, 150 rows per partition.
     */
    private void generateTestData() throws IOException {
        String[] partitions = { "dt=2026-04-13", "dt=2026-04-14", "dt=2026-04-15" };
        for (String partition : partitions) {
            File partDir = new File(warehouseDir, DATABASE + "/" + TABLE_NAME + "/" + partition);
            partDir.mkdirs();
            writeParquetFile(new File(partDir, "part-00000.parquet"), 100, 0);
            writeParquetFile(new File(partDir, "part-00001.parquet"), 50, 100);
        }
    }

    private void writeParquetFile(File file, int numRows, int startId) throws IOException {
        SimpleGroupFactory factory = new SimpleGroupFactory(PARQUET_SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toURI()))
                .withType(PARQUET_SCHEMA)
                .withConf(new Configuration())
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                int id = startId + i;
                Group group = factory.newGroup()
                    .append("event_id", "evt-" + id)
                    .append("user_id", "user-" + (id % 100))
                    .append("event_type", new String[] { "click", "view", "purchase" }[id % 3])
                    .append("timestamp", 1714500000L + id * 60)
                    .append("amount", (double) (id * 10));
                writer.write(group);
            }
        }
    }

    private void setupHiveMetastore() {
        hiveMetastore = new GenericContainer<>("apache/hive:4.0.1").withEnv("SERVICE_NAME", "metastore")
            .withEnv("DB_DRIVER", "derby")
            .withExposedPorts(9083)
            .withFileSystemBind(warehouseDir.getAbsolutePath(), WAREHOUSE_CONTAINER_PATH, BindMode.READ_WRITE)
            .waitingFor(Wait.forListeningPort());
        hiveMetastore.start();
    }

    private void registerTableAndPartitions() throws Exception {
        String host = hiveMetastore.getHost();
        int port = hiveMetastore.getMappedPort(9083);

        TTransport transport = new TSocket(host, port, 10000);
        transport.open();
        ThriftHiveMetastore.Client client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));

        try {
            Database db = new Database();
            db.setName(DATABASE);
            db.setLocationUri(WAREHOUSE_CONTAINER_PATH + "/" + DATABASE);
            try {
                client.create_database(db);
            } catch (Exception e) {
                if (!e.getClass().getSimpleName().contains("AlreadyExists")) throw e;
            }

            List<FieldSchema> cols = new ArrayList<>();
            cols.add(new FieldSchema("event_id", "string", ""));
            cols.add(new FieldSchema("user_id", "string", ""));
            cols.add(new FieldSchema("event_type", "string", ""));
            cols.add(new FieldSchema("timestamp", "bigint", ""));
            cols.add(new FieldSchema("amount", "double", ""));

            SerDeInfo serDeInfo = new SerDeInfo();
            serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");

            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(cols);
            sd.setLocation(WAREHOUSE_CONTAINER_PATH + "/" + DATABASE + "/" + TABLE_NAME);
            sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
            sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
            sd.setSerdeInfo(serDeInfo);

            Table table = new Table();
            table.setTableName(TABLE_NAME);
            table.setDbName(DATABASE);
            table.setSd(sd);
            table.setPartitionKeys(List.of(new FieldSchema("dt", "string", "")));
            table.setTableType("EXTERNAL_TABLE");
            table.setParameters(new HashMap<>());

            try {
                client.create_table(table);
            } catch (Exception e) {
                if (!e.getClass().getSimpleName().contains("AlreadyExists")) throw e;
            }

            String[] partitionValues = { "2026-04-13", "2026-04-14", "2026-04-15" };
            for (String dt : partitionValues) {
                StorageDescriptor partSd = new StorageDescriptor();
                partSd.setCols(cols);
                partSd.setLocation(WAREHOUSE_CONTAINER_PATH + "/" + DATABASE + "/" + TABLE_NAME + "/dt=" + dt);
                partSd.setInputFormat(sd.getInputFormat());
                partSd.setOutputFormat(sd.getOutputFormat());
                partSd.setSerdeInfo(serDeInfo);

                Partition partition = new Partition();
                partition.setValues(List.of(dt));
                partition.setDbName(DATABASE);
                partition.setTableName(TABLE_NAME);
                partition.setSd(partSd);

                try {
                    client.add_partition(partition);
                } catch (Exception e) {
                    if (!e.getClass().getSimpleName().contains("AlreadyExists")) throw e;
                }
            }
        } finally {
            transport.close();
        }
    }

    protected void waitForState(Callable<Boolean> checkState) throws Exception {
        assertBusy(() -> {
            if (checkState.call() == false) {
                fail("Provided state requirements not met");
            }
        }, 2, TimeUnit.MINUTES);
    }

    private static void deleteDirectory(File dir) {
        if (dir == null || !dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDirectory(f);
                else f.delete();
            }
        }
        dir.delete();
    }

    public static final class TestContainerThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("testcontainers-pull-watchdog-")
                || t.getName().startsWith("testcontainers-ryuk")
                || t.getName().startsWith("stream-poller-consumer");
        }
    }
}
