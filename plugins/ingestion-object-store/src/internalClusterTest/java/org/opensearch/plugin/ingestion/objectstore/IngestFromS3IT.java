/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.s3.S3RepositoryPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import static org.hamcrest.Matchers.is;

/**
 * Integration test for the {@code object_store} ingestion source against a registered {@code s3} repository backed by
 * a LocalStack S3 endpoint. It puts an ndjson object into an S3 bucket, registers an {@code s3} repository via
 * {@code repository-s3}, points an index's {@code object_store} source at that repository, and asserts the documents
 * become searchable.
 */
@ThreadLeakFilters(filters = TestContainerThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class IngestFromS3IT extends OpenSearchIntegTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String BASE_PATH = "dump";
    private static final String OBJECT_KEY = BASE_PATH + "/events.ndjson";
    private static final String CLIENT = "object_store";
    private static final String REPOSITORY = "object_store_s3_repo";
    private static final String INDEX = "test-s3-index";
    private static final String MAPPINGS = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}";

    /**
     * Pinned by manifest-list digest, not by tag alone: a tag is mutable, so pinning only {@code 4.14.0} would let a
     * republished tag change what the test pulls. The digest is the multi-architecture index, so it resolves on both
     * amd64 and arm64. {@code asCompatibleSubstituteFor} is required because the digest-qualified name no longer
     * matches the name {@link LocalStackContainer} expects.
     */
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse(
        "localstack/localstack:4.14.0@sha256:3ebc37595918b8accb852f8048fef2aff047d465167edd655528065b07bc364a"
    ).asCompatibleSubstituteFor("localstack/localstack");

    private static LocalStackContainer localstack;
    private S3Client s3Client;

    @BeforeClass
    public static void startLocalStack() {
        localstack = new LocalStackContainer(LOCALSTACK_IMAGE).withEnv("AWS_ACCESS_KEY_ID", "test")
            .withEnv("AWS_SECRET_ACCESS_KEY", "test")
            .withServices(LocalStackContainer.Service.S3);
        localstack.start();
    }

    @AfterClass
    public static void stopLocalStack() {
        if (localstack != null) {
            localstack.stop();
            localstack = null;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ObjectStorePlugin.class, S3RepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client." + CLIENT + ".access_key", localstack.getAccessKey());
        secureSettings.setString("s3.client." + CLIENT + ".secret_key", localstack.getSecretKey());
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("s3.client." + CLIENT + ".endpoint", localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString())
            .put("s3.client." + CLIENT + ".region", localstack.getRegion())
            .put("s3.client." + CLIENT + ".path_style_access", true)
            .setSecureSettings(secureSettings)
            .build();
    }

    @Before
    public void setupS3() {
        s3Client = S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .region(Region.of(localstack.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()))
            )
            .forcePathStyle(true)
            .build();

        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
            logger.info("bucket [{}] already exists on the shared LocalStack container", BUCKET);
        }
    }

    @After
    public void stopS3() {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    private String doc(String name, int age) {
        return String.format(Locale.ROOT, "{\"name\":\"%s\",\"age\":%d}", name, age);
    }

    private long putObject(String... docs) {
        StringBuilder body = new StringBuilder();
        for (String doc : docs) {
            body.append(doc).append("\n");
        }
        String content = body.toString();
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key(OBJECT_KEY).build(), RequestBody.fromString(content));
        return content.getBytes(StandardCharsets.UTF_8).length;
    }

    private void registerRepository() {
        AcknowledgedResponse response = client().admin()
            .cluster()
            .putRepository(
                new PutRepositoryRequest(REPOSITORY).type("s3")
                    .settings(
                        Settings.builder()
                            .put("bucket", BUCKET)
                            .put("client", CLIENT)
                            .put("base_path", BASE_PATH)
                            .put("readonly", true)
                            .build()
                    )
            )
            .actionGet();
        assertThat(response.isAcknowledged(), is(true));
    }

    public void testS3Ingestion() throws Exception {
        long size = putObject(doc("alice", 30), doc("bob", 35), doc("carol", 40));
        registerRepository();

        createIndex(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "OBJECT_STORE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.repository", REPOSITORY)
                .put("ingestion_source.param.format", "ndjson")
                .put("ingestion_source.param.shard_split", "equal_bytes")
                .put("ingestion_source.param.source_url", "events.ndjson")
                .put("ingestion_source.param.manifest", String.format(Locale.ROOT, "[{\"key\":\"events.ndjson\",\"size\":%d}]", size))
                .put("ingestion_source.mapper_type", "raw_payload")
                .put("index.replication.type", "SEGMENT")
                .build(),
            MAPPINGS
        );

        assertSearchableCount(3L);
    }

    public void testS3IngestionTwoShardEqualBytesSplit() throws Exception {
        long size = putObject(doc("alice", 30), doc("bob", 35), doc("carol", 40), doc("dave", 45));
        registerRepository();

        createIndex(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "OBJECT_STORE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.repository", REPOSITORY)
                .put("ingestion_source.param.format", "ndjson")
                .put("ingestion_source.param.shard_split", "equal_bytes")
                .put("ingestion_source.param.source_url", "events.ndjson")
                .put("ingestion_source.param.manifest", String.format(Locale.ROOT, "[{\"key\":\"events.ndjson\",\"size\":%d}]", size))
                .put("ingestion_source.mapper_type", "raw_payload")
                .put("index.replication.type", "SEGMENT")
                .build(),
            MAPPINGS
        );

        assertSearchableCount(4L);
    }

    private void assertSearchableCount(long expected) throws Exception {
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        assertBusy(() -> {
            refresh(INDEX);
            SearchResponse response = client().prepareSearch(INDEX).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(expected));
        }, 60, TimeUnit.SECONDS);
    }
}
