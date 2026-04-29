/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.catalog.MetadataClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.env.Environment;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.plugin.catalog.iceberg.credentials.CredentialsBuilder;
import org.opensearch.plugin.catalog.iceberg.credentials.IcebergClientCredentialsProvider;
import org.opensearch.plugin.catalog.iceberg.credentials.IcebergClientSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link MetadataClient} implementation backed by an Apache Iceberg REST catalog on
 * AWS S3 Tables.
 * <p>
 * One instance per node. Constructs a single {@link RESTCatalog} wired with the
 * plugin's credential resolution (see
 * {@link org.opensearch.plugin.catalog.iceberg.credentials}) and a {@link S3FileIO} for
 * data-plane access. The credentials provider is handed to Iceberg via its
 * {@code client.credentials-provider} reflection hook — the same hook covers both
 * {@link S3FileIO} and the REST catalog's {@code RESTSigV4Signer}.
 * <p>
 * This PR implements the catalog operations needed by core's orchestration:
 * {@link #initialize}, {@link #finalizePublish}, {@link #indexExists}, and {@link #close}.
 * {@link #getMetadata} returns {@code null} for now — IndexMetadata persistence (for
 * cold-tier restore) is deferred to a follow-up PR that will use an S3 sidecar object
 * or an Iceberg Puffin file rather than a table property, to avoid Iceberg's
 * metadata.json rewrite cost on every commit. The per-shard {@link #publish} still
 * throws {@link UnsupportedOperationException}; it lands in a follow-up.
 *
 * @opensearch.experimental
 */
public class IcebergMetadataClient implements MetadataClient {

    private static final Logger logger = LogManager.getLogger(IcebergMetadataClient.class);

    /** Namespace under which all OpenSearch-published Iceberg tables are created. */
    public static final String NAMESPACE = "opensearch";

    /** Max attempts for {@link #finalizePublish} rollback on a {@link CommitFailedException}. */
    private static final int FINALIZE_MAX_ATTEMPTS = 3;

    /** Initial backoff delay in millis for {@link #finalizePublish} retry. */
    private static final long FINALIZE_INITIAL_BACKOFF_MILLIS = 1000L;

    private final IcebergCatalogRepository repository;
    private final Environment environment;
    private final RESTCatalog catalog;
    private final AwsCredentialsProvider credentialsProvider;
    private final String registryKey;

    /**
     * Creates a new client bound to the given repository. Resolves credentials, registers
     * them with the {@link IcebergClientCredentialsProvider} registry, builds a
     * {@link RESTCatalog} pointing at the configured endpoint, and loads the OpenSearch
     * namespace.
     *
     * @param repository   catalog repository holding the warehouse settings
     * @param environment  node environment (for keystore secrets + config dir resolution)
     */
    public IcebergMetadataClient(IcebergCatalogRepository repository, Environment environment) {
        this.repository = repository;
        this.environment = environment;

        IcebergClientSettings clientSettings = IcebergClientSettings.load(repository, environment);
        this.credentialsProvider = CredentialsBuilder.build(clientSettings);
        this.registryKey = IcebergClientCredentialsProvider.register(credentialsProvider);

        Map<String, String> properties = buildCatalogProperties();
        try {
            RESTCatalog c = new RESTCatalog();
            c.initialize(NAMESPACE, properties);
            this.catalog = c;
        } catch (RuntimeException e) {
            IcebergClientCredentialsProvider.deregister(registryKey);
            throw e;
        }
    }

    private Map<String, String> buildCatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(org.apache.iceberg.CatalogProperties.URI, repository.getCatalogEndpoint());
        props.put(org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION, repository.getBucketArn());
        props.put(org.apache.iceberg.CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());

        // rest.sigv4-enabled is a package-private string in HTTPClient; AwsProperties exposes
        // the companion signer-region / signing-name keys as public constants.
        props.put("rest.sigv4-enabled", "true");
        props.put(AwsProperties.REST_SIGNER_REGION, repository.getRegion());
        props.put(AwsProperties.REST_SIGNING_NAME, "s3tables");

        props.put(AwsClientProperties.CLIENT_REGION, repository.getRegion());
        props.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, IcebergClientCredentialsProvider.class.getName());
        props.put(IcebergClientCredentialsProvider.REGISTRY_KEY_PROPERTY, registryKey);
        return props;
    }

    @Override
    public String initialize(String indexName, IndexMetadata indexMetadata) throws IOException {
        TableIdentifier id = tableId(indexName);
        Table table;
        Snapshot currentSnapshot;

        if (catalog.tableExists(id)) {
            table = catalog.loadTable(id);
            currentSnapshot = table.currentSnapshot();
            logger.info("Catalog table [{}] exists; reusing current schema", id);
        } else {
            Schema schema = OpenSearchSchemaInference.inferSchema(indexMetadata);
            PartitionSpec spec = OpenSearchSchemaInference.partitionSpec(schema);
            table = catalog.createTable(id, schema, spec);
            currentSnapshot = null;
            logger.info("Created catalog table [{}] for index [{}]", id, indexName);
        }

        return currentSnapshot == null ? null : Long.toString(currentSnapshot.snapshotId());
    }

    @Override
    public void publish(String indexName, RemoteSegmentStoreDirectory remoteDirectory, int shardId) throws IOException {
        throw new UnsupportedOperationException("publish is not yet implemented");
    }

    @Override
    public void finalizePublish(String indexName, boolean success, String savedSnapshotId) throws IOException {
        TableIdentifier id = tableId(indexName);
        if (!catalog.tableExists(id)) {
            logger.warn("finalizePublish called for unknown table [{}]; skipping", id);
            return;
        }

        if (success) {
            // No catalog-side bookkeeping. Commit history lives on the Iceberg snapshot
            // itself; orchestration completion lives in the cluster-state entry.
            logger.info("finalizePublish(success) for [{}] — no-op", indexName);
            return;
        }

        if (savedSnapshotId == null) {
            // First-ever publish failed before creating any snapshot — nothing to roll back.
            logger.warn("finalizePublish(failure) for [{}] with no savedSnapshotId — nothing to roll back", indexName);
            return;
        }
        long rollbackTo;
        try {
            rollbackTo = Long.parseLong(savedSnapshotId);
        } catch (NumberFormatException e) {
            throw new IOException("invalid savedSnapshotId [" + savedSnapshotId + "] for [" + indexName + "]", e);
        }

        Table table = catalog.loadTable(id);
        // Idempotent — rollbackTo is a no-op when the table is already at that snapshot.
        commitWithRetry(table, t -> {
            Snapshot current = t.currentSnapshot();
            if (current != null && current.snapshotId() == rollbackTo) {
                return;
            }
            t.manageSnapshots().rollbackTo(rollbackTo).commit();
        }, "rolling back [" + indexName + "] to snapshot [" + savedSnapshotId + "]");
    }

    /**
     * Returns {@code null}. IndexMetadata round-trip via the catalog is not yet
     * implemented — see the class javadoc for the rationale and the planned follow-up
     * (S3 sidecar or Puffin file).
     */
    @Override
    public IndexMetadata getMetadata(String indexName) throws IOException {
        return null;
    }

    @Override
    public boolean indexExists(String indexName) throws IOException {
        return catalog.tableExists(tableId(indexName));
    }

    @Override
    public void close() throws IOException {
        try {
            catalog.close();
        } catch (Exception e) {
            throw new IOException("failed to close Iceberg catalog", e);
        } finally {
            IcebergClientCredentialsProvider.deregister(registryKey);
            if (credentialsProvider instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) credentialsProvider).close();
                } catch (IOException | RuntimeException e) {
                    throw e instanceof IOException ? (IOException) e : new IOException(e);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
    }

    private static TableIdentifier tableId(String indexName) {
        return TableIdentifier.of(Namespace.of(NAMESPACE), indexName);
    }

    /** Runs the given commit body with bounded exponential backoff on {@link CommitFailedException}. */
    private static void commitWithRetry(Table table, TableUpdate body, String description) throws IOException {
        long backoff = FINALIZE_INITIAL_BACKOFF_MILLIS;
        CommitFailedException last = null;
        for (int attempt = 1; attempt <= FINALIZE_MAX_ATTEMPTS; attempt++) {
            try {
                body.apply(table);
                return;
            } catch (CommitFailedException e) {
                last = e;
                final int attemptNumber = attempt;
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Iceberg commit failed on attempt [{}/{}] for [{}]",
                        attemptNumber,
                        FINALIZE_MAX_ATTEMPTS,
                        description
                    ),
                    e
                );
                if (attempt == FINALIZE_MAX_ATTEMPTS) {
                    break;
                }
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while retrying " + description, ie);
                }
                backoff *= 2;
                table.refresh();
            }
        }
        throw new IOException("failed to commit after " + FINALIZE_MAX_ATTEMPTS + " attempts: " + description, last);
    }

    @FunctionalInterface
    private interface TableUpdate {
        void apply(Table table);
    }

    // Visible for tests.
    String getRegistryKey() {
        return registryKey;
    }

    // Visible for tests.
    Environment getEnvironment() {
        return environment;
    }
}
