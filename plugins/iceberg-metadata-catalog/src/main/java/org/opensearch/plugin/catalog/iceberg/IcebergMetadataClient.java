/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.catalog.MetadataClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.env.Environment;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.plugin.catalog.iceberg.credentials.CredentialsBuilder;
import org.opensearch.plugin.catalog.iceberg.credentials.IcebergClientCredentialsProvider;
import org.opensearch.plugin.catalog.iceberg.credentials.IcebergClientSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /** Key for the Iceberg table property that stores the source OpenSearch index UUID. */
    static final String PROPERTY_INDEX_UUID = "opensearch.index_uuid";

    /** Max attempts for {@link #finalizePublish} rollback on a {@link CommitFailedException}. */
    private static final int FINALIZE_MAX_ATTEMPTS = 3;

    /** Initial backoff delay in millis for {@link #finalizePublish} retry. */
    private static final long FINALIZE_INITIAL_BACKOFF_MILLIS = 1000L;

    private final IcebergCatalogRepository repository;
    private final Environment environment;
    private final Catalog catalog;
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

    /**
     * Test-only constructor. Accepts a pre-built catalog and bypasses credential resolution
     * and REST endpoint setup. Package-private so tests can inject an in-memory or mocked
     * {@link Catalog} without reaching the network.
     */
    IcebergMetadataClient(Catalog catalog) {
        this.repository = null;
        this.environment = null;
        this.catalog = catalog;
        this.credentialsProvider = null;
        this.registryKey = null;
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
        String expectedIndexUUID = indexMetadata.getIndexUUID();
        Table table;
        Snapshot currentSnapshot;

        if (catalog.tableExists(id)) {
            table = catalog.loadTable(id);
            String storedIndexUUID = table.properties().get(PROPERTY_INDEX_UUID);
            if (storedIndexUUID == null) {
                // Table exists but was not created by this plugin (or predates the property).
                // Fail closed: publishing into it would mix two indices' data under incompatible
                // partition values.
                throw new IOException(
                    "Iceberg table ["
                        + id
                        + "] is missing required property ["
                        + PROPERTY_INDEX_UUID
                        + "]. Table was not created by this plugin or is from an earlier incompatible version."
                );
            }
            if (!storedIndexUUID.equals(expectedIndexUUID)) {
                // Namespace collision: caller's index UUID does not match the table's stamped UUID.
                // Reject so we don't silently merge two unrelated indices that happen to share a name.
                throw new IOException(
                    "Iceberg table ["
                        + id
                        + "] belongs to a different OpenSearch index (stored UUID ["
                        + storedIndexUUID
                        + "] does not match current index UUID ["
                        + expectedIndexUUID
                        + "])."
                );
            }
            currentSnapshot = table.currentSnapshot();
            logger.info("Catalog table [{}] exists with matching index UUID; reusing", id);
        } else {
            Schema schema = OpenSearchSchemaInference.inferSchema(indexMetadata);
            PartitionSpec spec = OpenSearchSchemaInference.partitionSpec(schema);
            Map<String, String> tableProperties = new HashMap<>();
            tableProperties.put(PROPERTY_INDEX_UUID, expectedIndexUUID);
            table = catalog.createTable(id, schema, spec, tableProperties);
            currentSnapshot = null;
            logger.info("Created catalog table [{}] for index [{}] with index UUID [{}]", id, indexName, expectedIndexUUID);
        }

        return currentSnapshot == null ? null : Long.toString(currentSnapshot.snapshotId());
    }

    @Override
    public void publish(String indexName, RemoteSegmentStoreDirectory remoteDirectory, int shardId) throws IOException {
        TableIdentifier id = tableId(indexName);
        PublishContext ctx = PublishContext.create(catalog, id, indexName, shardId);

        List<String> parquetFiles = discoverParquetFiles(remoteDirectory);
        if (parquetFiles.isEmpty()) {
            logger.info("No parquet files discovered for shard [{}][{}]; skipping publish", indexName, shardId);
            return;
        }

        List<DataFile> uploaded = new ArrayList<>(parquetFiles.size());
        FileIO fileIO = ctx.table().io();
        for (String remoteFilename : parquetFiles) {
            uploaded.add(ParquetFileUploader.upload(ctx, remoteDirectory, remoteFilename, fileIO));
        }

        appendWithRetry(ctx, uploaded);
    }

    /**
     * Discovers committed parquet files for this shard via
     * {@link RemoteSegmentStoreDirectory#readLatestMetadataFile()}. Returns the logical
     * remote-store filenames (UUID-suffixed, as stored in the metadata file). An empty
     * result means the shard has no parquet files committed yet — a valid no-op.
     * <p>
     * The remote-store layout suffixes each filename with {@code "__<UUID>"} after the
     * extension (e.g. {@code _0.parquet__gX7bNIIBrs0AUNsR2yEG}), so a plain
     * {@link String#endsWith(String) endsWith(".parquet")} check does not work. We
     * strip the suffix at its first {@code "__"} marker before comparing the extension.
     */
    private static List<String> discoverParquetFiles(RemoteSegmentStoreDirectory remoteDirectory) throws IOException {
        RemoteSegmentMetadata metadata = remoteDirectory.readLatestMetadataFile();
        if (metadata == null) {
            return Collections.emptyList();
        }
        List<String> out = new ArrayList<>();
        for (String filename : metadata.getMetadata().keySet()) {
            if (isParquetFilename(filename)) {
                out.add(filename);
            }
        }
        return out;
    }

    /**
     * Returns {@code true} if the remote-store filename represents a parquet file,
     * ignoring the {@code "__<UUID>"} suffix appended by the remote store.
     */
    static boolean isParquetFilename(String filename) {
        int suffixIndex = filename.indexOf("__");
        String logicalName = suffixIndex >= 0 ? filename.substring(0, suffixIndex) : filename;
        return logicalName.endsWith(".parquet");
    }

    /**
     * Registers the uploaded {@code DataFile}s in the Iceberg table via {@code AppendFiles}.
     * Applies the filter-before-commit pattern: on each attempt, refresh the table, drop any
     * files already tracked in this shard's partition, and skip the commit entirely when the
     * filtered set is empty. Combined with the content-addressed warehouse paths (layer 3)
     * and Iceberg's {@code CommitFailedException} CAS (layer 2), retries produce no duplicate
     * rows.
     */
    private void appendWithRetry(PublishContext ctx, List<DataFile> candidates) throws IOException {
        commitWithRetry(ctx.table(), t -> {
            t.refresh();
            Set<String> tracked = readTrackedPaths(t, ctx.indexUUID(), ctx.shardId());
            List<DataFile> newFiles = new ArrayList<>(candidates.size());
            for (DataFile df : candidates) {
                if (!tracked.contains(df.path().toString())) {
                    newFiles.add(df);
                }
            }
            if (newFiles.isEmpty()) {
                logger.info(
                    "All {} parquet files for [{}][{}] already tracked; no-op commit",
                    candidates.size(),
                    ctx.indexName(),
                    ctx.shardId()
                );
                return;
            }
            AppendFiles append = t.newAppend();
            for (DataFile df : newFiles) {
                append.appendFile(df);
            }
            append.commit();
            logger.info(
                "Appended {} new parquet files to [{}][{}] ({} already tracked skipped)",
                newFiles.size(),
                ctx.indexName(),
                ctx.shardId(),
                candidates.size() - newFiles.size()
            );
        }, "appending " + candidates.size() + " parquet files to [" + ctx.indexName() + "][" + ctx.shardId() + "]");
    }

    /**
     * Returns the set of file paths already registered in the Iceberg table under this
     * publish's partition. Iterates all tracked files and filters in-memory by partition
     * values read from {@code DataFile.partition()} — relying on Iceberg's partition-filter
     * pushdown via {@code TableScan.filter(Expressions.equal(...))} does not work here,
     * because those predicates are column-stats-based for data columns, not partition-value
     * matchers. The number of parquet files per shard is small (at most the shard's active
     * segment count), so iterating all tracked entries for one partition is acceptable.
     */
    private static Set<String> readTrackedPaths(Table table, String indexUUID, int shardId) throws IOException {
        Set<String> paths = new HashSet<>();
        if (table.currentSnapshot() == null) {
            return paths;
        }
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                StructLike partition = task.file().partition();
                if (partitionMatches(partition, indexUUID, shardId)) {
                    paths.add(task.file().path().toString());
                }
            }
        }
        return paths;
    }

    /**
     * Returns {@code true} if the file's partition values correspond to the given
     * {@code (index_uuid, shard_id)}. Relies on the partition spec ordering
     * {@code identity(index_uuid), identity(shard_id)} established by
     * {@link OpenSearchSchemaInference#partitionSpec}.
     */
    private static boolean partitionMatches(StructLike partition, String indexUUID, int shardId) {
        if (partition.size() < 2) {
            return false;
        }
        Object storedUUID = partition.get(0, Object.class);
        Object storedShard = partition.get(1, Object.class);
        return indexUUID.equals(String.valueOf(storedUUID)) && Integer.valueOf(shardId).equals(storedShard);
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
     *
     * @param indexName the OpenSearch index name (ignored until the follow-up lands)
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
            if (catalog instanceof AutoCloseable) {
                ((AutoCloseable) catalog).close();
            }
        } catch (IOException | RuntimeException e) {
            throw e instanceof IOException ? (IOException) e : new IOException("failed to close Iceberg catalog", e);
        } catch (Exception e) {
            throw new IOException("failed to close Iceberg catalog", e);
        } finally {
            if (registryKey != null) {
                IcebergClientCredentialsProvider.deregister(registryKey);
            }
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
        void apply(Table table) throws IOException;
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
