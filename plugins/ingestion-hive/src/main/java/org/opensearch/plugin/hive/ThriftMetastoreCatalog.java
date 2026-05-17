/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.opensearch.plugin.hive.metastore.FieldSchema;
import org.opensearch.plugin.hive.metastore.GetTableRequest;
import org.opensearch.plugin.hive.metastore.GetTableResult;
import org.opensearch.plugin.hive.metastore.Partition;
import org.opensearch.plugin.hive.metastore.Table;
import org.opensearch.plugin.hive.metastore.ThriftHiveMetastore;
import org.opensearch.secure_sm.AccessController;

import javax.security.sasl.SaslException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive Metastore catalog implementation using Thrift RPC.
 * Supports framed/unframed transport and Kerberos (SASL/GSSAPI) authentication.
 */
public class ThriftMetastoreCatalog implements MetastoreCatalog {

    private static final Logger logger = LogManager.getLogger(ThriftMetastoreCatalog.class);

    private final HiveSourceConfig config;
    private ThriftHiveMetastore.Client client;
    private TTransport transport;

    public ThriftMetastoreCatalog(HiveSourceConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws IOException {
        try {
            connectInternal();
        } catch (TTransportException e) {
            throw new IOException("Failed to connect to Metastore: " + e.getMessage(), e);
        }
    }

    @Override
    public void reconnect() throws IOException {
        close();
        connect();
    }

    @Override
    public TableInfo getTableInfo(String database, String table) throws IOException {
        try {
            GetTableRequest req = new GetTableRequest();
            req.setDbName(database);
            req.setTblName(table);
            GetTableResult result = client.get_table_req(req);
            Table t = result.getTable();

            List<ColumnInfo> columns = t.getSd()
                .getCols()
                .stream()
                .map(fs -> new ColumnInfo(fs.getName(), fs.getType()))
                .collect(Collectors.toList());
            List<String> partitionKeys = t.getPartitionKeys() != null
                ? t.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList())
                : Collections.emptyList();

            return new TableInfo(t.getSd().getInputFormat(), partitionKeys, columns);
        } catch (TException e) {
            throw new IOException("Failed to get table info: " + e.getMessage(), e);
        }
    }

    @Override
    public List<PartitionInfo> getAllPartitions(String database, String table) throws IOException {
        try {
            List<Partition> partitions = client.get_partitions(database, table, (short) -1);
            return partitions.stream().map(this::toPartitionInfo).collect(Collectors.toList());
        } catch (TException e) {
            throw new IOException("Failed to get partitions: " + e.getMessage(), e);
        }
    }

    @Override
    public List<PartitionInfo> getPartitionsByFilter(String database, String table, String filter) throws IOException {
        try {
            List<Partition> partitions = client.get_partitions_by_filter(database, table, filter, (short) -1);
            return partitions.stream().map(this::toPartitionInfo).collect(Collectors.toList());
        } catch (TException e) {
            throw new IOException("Failed to get partitions by filter: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                logger.debug("Error closing metastore transport", e);
            }
            transport = null;
            client = null;
        }
    }

    private PartitionInfo toPartitionInfo(Partition p) {
        return new PartitionInfo(p.getValues(), p.getSd().getLocation(), p.getCreateTime());
    }

    private void connectInternal() throws TTransportException {
        String uri = config.getMetastoreUri().replace("thrift://", "");
        String[] hostPort = uri.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        if (config.getAuthMode() == HiveSourceConfig.AuthMode.KERBEROS) {
            loginWithKerberos();
        }

        TTransportException lastException = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                if (attempt > 0) {
                    logger.info("Retrying Metastore connection (attempt {}/{})", attempt, config.getMaxRetries());
                    Thread.sleep(config.getRetryIntervalMillis());
                }

                TSocket socket = new TSocket(host, port, config.getConnectTimeoutMillis());
                TTransport t;
                if (config.getTransportMode() == HiveSourceConfig.TransportMode.FRAMED) {
                    t = new TFramedTransport(socket);
                } else {
                    t = socket;
                }

                if (config.getAuthMode() == HiveSourceConfig.AuthMode.KERBEROS) {
                    t = createSaslTransport(t, host);
                    final TTransport saslTransport = t;
                    try {
                        UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Void>) () -> {
                            saslTransport.open();
                            return null;
                        });
                    } catch (Exception e) {
                        throw new TTransportException("SASL open failed: " + e.getMessage(), e);
                    }
                } else {
                    t.open();
                }

                transport = t;
                client = new ThriftHiveMetastore.Client(new TBinaryProtocol(t));
                logger.info(
                    "Connected to Hive Metastore at {}:{} (transport={}, auth={})",
                    host,
                    port,
                    config.getTransportMode(),
                    config.getAuthMode()
                );
                return;
            } catch (TTransportException e) {
                lastException = e;
                logger.warn("Failed to connect to Metastore (attempt {}): {}", attempt + 1, e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TTransportException("Interrupted during retry", e);
            }
        }
        throw new TTransportException("Failed to connect to Metastore after " + config.getMaxRetries() + " retries", lastException);
    }

    private void loginWithKerberos() throws TTransportException {
        try {
            AccessController.doPrivilegedChecked(() -> {
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(config.getKerberosPrincipal(), config.getKerberosKeytabPath());
                logger.info("Kerberos login successful for principal {}", config.getKerberosPrincipal());
            });
        } catch (Exception e) {
            throw new TTransportException("Kerberos login failed: " + e.getMessage(), e);
        }
    }

    private TTransport createSaslTransport(TTransport baseTransport, String host) throws TTransportException {
        try {
            String servicePrincipal = config.getMetastoreServicePrincipal();
            if (servicePrincipal == null) {
                servicePrincipal = "hive/" + host;
            } else {
                servicePrincipal = SecurityUtil.getServerPrincipal(servicePrincipal, host);
            }
            KerberosName kerberosName = new KerberosName(servicePrincipal);
            String saslServiceName = kerberosName.getServiceName();
            String saslHost = kerberosName.getHostName();

            return new TSaslClientTransport(
                "GSSAPI",
                null,
                saslServiceName,
                saslHost,
                Map.of("javax.security.sasl.qop", "auth", "javax.security.sasl.server.authentication", "true"),
                null,
                baseTransport
            );
        } catch (SaslException e) {
            throw new TTransportException("Failed to create SASL transport: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new TTransportException("Failed to resolve service principal: " + e.getMessage(), e);
        }
    }
}
