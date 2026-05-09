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

import org.apache.hadoop.minikdc.MiniKdc;
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
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Integration test for Hive ingestion with Kerberos authentication.
 * Uses MiniKdc (in-JVM KDC) and a Hive Metastore container configured for SASL/Kerberos.
 */
@SuppressForbidden(reason = "Parquet and Hadoop APIs require java.io.File")
@ThreadLeakFilters(filters = IngestFromHiveKerberosIT.TestContainerThreadLeakFilter.class)
public class IngestFromHiveKerberosIT extends OpenSearchSingleNodeTestCase {

    private static final String DATABASE = "test_db";
    private static final String TABLE_NAME = "events";

    private MiniKdc kdc;
    private File keytabFile;
    private File warehouseDir;
    private GenericContainer<?> kerberizedMetastore;
    private String indexName = "hive-kerberos-test-index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(HiveIngestionPlugin.class);
    }

    @Before
    public void setup() throws Exception {
        warehouseDir = createTempDir().toFile();
        generateTestData();
        startMiniKdc();
        startKerberizedMetastore();
    }

    @After
    public void cleanup() throws Exception {
        // Delete index first to stop the poller before shutting down KDC/Metastore
        try {
            client().admin().indices().prepareDelete("hive-kerberos-test-index").get();
        } catch (Exception e) {
            // index may not exist if test failed early
        }
        // Wait for poller thread to terminate
        assertBusy(() -> {
            Thread[] threads = new Thread[Thread.activeCount()];
            Thread.enumerate(threads);
            for (Thread t : threads) {
                if (t != null && t.getName().contains("stream-poller")) {
                    fail("Poller thread still alive: " + t.getName());
                }
            }
        });
        if (kerberizedMetastore != null) kerberizedMetastore.stop();
        if (kdc != null) kdc.stop();
    }

    public void testKerberosIngestion() throws Exception {
        String metastoreUri = "thrift://" + kerberizedMetastore.getHost() + ":" + kerberizedMetastore.getMappedPort(9083);

        // Register table via SASL-authenticated connection
        registerTableWithKerberos(metastoreUri);

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
                .put("ingestion_source.mapper_type", "field_mapping")
                .put("ingestion_source.param.authentication", "kerberos")
                .put("ingestion_source.param.kerberos_principal", "client@" + kdc.getRealm())
                .put("ingestion_source.param.kerberos_keytab", keytabFile.getAbsolutePath())
                .put("ingestion_source.param.metastore_service_principal", "hive/localhost@" + kdc.getRealm())
                .put("index.replication.type", "SEGMENT")
                .build()
        );
        ensureGreen(indexName);

        waitForState(() -> {
            SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            return response.getHits().getTotalHits().value() == 450;
        });
    }

    private void startMiniKdc() throws Exception {
        Properties conf = MiniKdc.createConf();
        File kdcDir = createTempDir().toFile();
        kdc = new MiniKdc(conf, kdcDir);
        kdc.start();

        // Create principals and keytab
        keytabFile = new File(warehouseDir, "test.keytab");
        kdc.createPrincipal(keytabFile, "client", "hive/localhost");

        logger.info("MiniKdc started on port {}, realm={}", kdc.getPort(), kdc.getRealm());
    }

    private void startKerberizedMetastore() throws Exception {
        // Generate hive-site.xml for Kerberos-enabled Metastore
        File hiveSiteXml = new File(warehouseDir, "hive-site.xml");
        java.nio.file.Files.writeString(
            hiveSiteXml.toPath(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<configuration>\n"
                + "  <property><name>metastore.sasl.enabled</name><value>true</value></property>\n"
                + "  <property><name>metastore.kerberos.principal</name><value>hive/localhost@"
                + kdc.getRealm()
                + "</value></property>\n"
                + "  <property><name>metastore.kerberos.keytab.file</name><value>/opt/keytabs/test.keytab</value></property>\n"
                + "  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>\n"
                + "</configuration>\n"
        );

        // Generate core-site.xml
        File coreSiteXml = new File(warehouseDir, "core-site.xml");
        java.nio.file.Files.writeString(
            coreSiteXml.toPath(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<configuration>\n"
                + "  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>\n"
                + "</configuration>\n"
        );

        // Copy krb5.conf generated by MiniKdc and rewrite KDC address for container access
        File krb5Conf = new File(System.getProperty("java.security.krb5.conf"));
        File containerKrb5Conf = new File(warehouseDir, "krb5.conf");
        String krb5Content = java.nio.file.Files.readString(krb5Conf.toPath());
        // Container accesses KDC via host.docker.internal instead of localhost
        krb5Content = krb5Content.replace("localhost", "host.docker.internal");
        java.nio.file.Files.writeString(containerKrb5Conf.toPath(), krb5Content);

        kerberizedMetastore = new GenericContainer<>("apache/hive:4.0.1").withExposedPorts(9083)
            .withFileSystemBind(warehouseDir.getAbsolutePath(), warehouseDir.getAbsolutePath(), BindMode.READ_WRITE)
            .withFileSystemBind(keytabFile.getAbsolutePath(), "/opt/keytabs/test.keytab", BindMode.READ_ONLY)
            .withFileSystemBind(hiveSiteXml.getAbsolutePath(), "/opt/hive/conf/hive-site.xml", BindMode.READ_ONLY)
            .withFileSystemBind(hiveSiteXml.getAbsolutePath(), "/opt/hive/conf/metastore-site.xml", BindMode.READ_ONLY)
            .withFileSystemBind(coreSiteXml.getAbsolutePath(), "/opt/hive/conf/core-site.xml", BindMode.READ_ONLY)
            .withFileSystemBind(containerKrb5Conf.getAbsolutePath(), "/etc/krb5.conf", BindMode.READ_ONLY)
            .withEnv("SERVICE_NAME", "metastore")
            .withEnv("DB_DRIVER", "derby")
            .withExtraHost("host.docker.internal", "host-gateway")
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(java.time.Duration.ofMinutes(3));
        kerberizedMetastore.start();
    }

    private void registerTableWithKerberos(String metastoreUri) throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
        org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
            .loginUserFromKeytabAndReturnUGI("client@" + kdc.getRealm(), keytabFile.getAbsolutePath());

        ugi.doAs((java.security.PrivilegedExceptionAction<Void>) () -> {
            String host = kerberizedMetastore.getHost();
            int port = kerberizedMetastore.getMappedPort(9083);

            TSocket socket = new TSocket(host, port, 10000);
            TTransport saslTransport = new org.apache.thrift.transport.TSaslClientTransport(
                "GSSAPI",
                null,
                "hive",
                "localhost",
                new java.util.HashMap<>(),
                null,
                socket
            );
            saslTransport.open();
            ThriftHiveMetastore.Client client = new ThriftHiveMetastore.Client(new TBinaryProtocol(saslTransport));

            try {
                Database db = new Database();
                db.setName(DATABASE);
                db.setLocationUri(warehouseDir.getAbsolutePath() + "/" + DATABASE);
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
                sd.setLocation(warehouseDir.getAbsolutePath() + "/" + DATABASE + "/" + TABLE_NAME);
                sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
                sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
                sd.setSerdeInfo(serDeInfo);

                Table table = new Table();
                table.setTableName(TABLE_NAME);
                table.setDbName(DATABASE);
                table.setSd(sd);
                table.setPartitionKeys(List.of(new FieldSchema("dt", "string", "")));
                table.setTableType("EXTERNAL_TABLE");
                table.setParameters(new java.util.HashMap<>());
                try {
                    client.create_table(table);
                } catch (Exception e) {
                    if (!e.getClass().getSimpleName().contains("AlreadyExists")) throw e;
                }

                for (String dt : new String[] { "2026-04-13", "2026-04-14", "2026-04-15" }) {
                    StorageDescriptor partSd = new StorageDescriptor();
                    partSd.setCols(cols);
                    partSd.setLocation(warehouseDir.getAbsolutePath() + "/" + DATABASE + "/" + TABLE_NAME + "/dt=" + dt);
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
                saslTransport.close();
            }
            return null;
        });
    }

    private void generateTestData() throws IOException {
        String[] partitions = { "dt=2026-04-13", "dt=2026-04-14", "dt=2026-04-15" };
        for (String partition : partitions) {
            File partDir = new File(warehouseDir, DATABASE + "/" + TABLE_NAME + "/" + partition);
            partDir.mkdirs();
            writeParquetFile(new File(partDir, "part-00000.parquet"), 0);
            writeParquetFile(new File(partDir, "part-00001.parquet"), 75);
        }
    }

    private void writeParquetFile(File file, int startIdx) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message event {
              required binary event_id (UTF8);
              required binary user_id (UTF8);
              required binary event_type (UTF8);
              required int64 timestamp;
              required double amount;
            }""");
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        org.apache.parquet.io.OutputFile outputFile = new org.apache.parquet.io.LocalOutputFile(file.toPath());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile).withType(schema).build()) {
            for (int i = startIdx; i < startIdx + 75; i++) {
                Group group = factory.newGroup();
                group.append("event_id", "evt-" + i);
                group.append("user_id", "user-" + (i % 10));
                group.append("event_type", i % 2 == 0 ? "click" : "view");
                group.append("timestamp", 1700000000L + i);
                group.append("amount", i * 1.5);
                writer.write(group);
            }
        }
    }

    protected void waitForState(Callable<Boolean> checkState) throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.SECONDS.toMillis(60)) {
            if (checkState.call()) return;
            Thread.sleep(500);
        }
        assertTrue("Provided state requirements not met", checkState.call());
    }

    public static class TestContainerThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("testcontainers-")
                || t.getName().contains("StatisticsDataReferenceCleaner")
                || t.getName().startsWith("ducttape")
                || t.getName().startsWith("MiniKdc");
        }
    }
}
