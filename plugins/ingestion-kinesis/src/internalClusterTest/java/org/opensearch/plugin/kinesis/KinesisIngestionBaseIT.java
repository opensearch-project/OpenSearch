/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base test class for kinesis ingestion tests
 */
@ThreadLeakFilters(filters = TestContainerThreadLeakFilter.class)
public class KinesisIngestionBaseIT extends OpenSearchIntegTestCase {
    static final String streamName = "test";
    static final String indexName = "testindex";
    static final String mapping = "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}}";
    static final long defaultMessageTimestamp = 1739459500000L;

    protected LocalStackContainer localstack;
    protected KinesisClient kinesisClient;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(KinesisPlugin.class);
    }

    @Before
    protected void setup() throws InterruptedException {
        setupKinesis();
    }

    @After
    protected void cleanup() {
        stopKinesis();
    }

    private void setupKinesis() throws InterruptedException {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest")).withServices(
            LocalStackContainer.Service.KINESIS
        );
        localstack.start();

        // Initialize AWS Kinesis Client with LocalStack endpoint
        kinesisClient = KinesisClient.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.KINESIS))
            .region(Region.of(localstack.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()))
            )
            .build();

        // Create a stream
        kinesisClient.createStream(CreateStreamRequest.builder().streamName(streamName).shardCount(1).build());

        // sleep for a while to allow the stream to be created
        Thread.sleep(500);
    }

    private void stopKinesis() {
        if (kinesisClient != null) {
            kinesisClient.close();
        }

        if (localstack != null) {
            localstack.stop();
            localstack = null;
        }
    }

    protected String produceData(String id, String name, String age) {
        return produceData(id, name, age, defaultMessageTimestamp);
    }

    protected String produceData(String id, String name, String age, long timestamp) {
        String payload = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\", \"_op_type\":\"index\",\"_source\":{\"name\":\"%s\", \"age\": %s}}",
            id,
            name,
            age
        );

        PutRecordResponse response = kinesisClient.putRecord(
            PutRecordRequest.builder().streamName(streamName).data(SdkBytes.fromUtf8String(payload)).partitionKey(id).build()
        );

        return response.sequenceNumber();
    }

    protected void waitForSearchableDocs(long docCount, List<String> nodes) throws Exception {
        assertBusy(() -> {
            for (String node : nodes) {
                final SearchResponse response = client(node).prepareSearch(indexName).setSize(0).setPreference("_only_local").get();
                final long hits = response.getHits().getTotalHits().value();
                if (hits < docCount) {
                    fail("Expected search hits on node: " + node + " to be at least " + docCount + " but was: " + hits);
                }
            }
        }, 1, TimeUnit.MINUTES);
    }
}
