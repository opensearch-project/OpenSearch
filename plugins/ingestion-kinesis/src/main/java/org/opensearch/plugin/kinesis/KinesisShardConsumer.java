/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.opensearch.plugin.kinesis.SequenceNumber.NON_EXISTING_SEQUENCE_NUMBER;
import static software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create;

/**
 * Kafka consumer to read messages from a Kafka partition
 */
@SuppressWarnings("removal")
public class KinesisShardConsumer implements IngestionShardConsumer<SequenceNumber, KinesisMessage> {
    private static final Logger logger = LogManager.getLogger(KinesisShardConsumer.class);

    /**
     * The Kinesis consumer
     */
    ;
    private KinesisClient kinesisClient;
    private String lastFetchedSequenceNumber = "";
    final String clientId;
    final String kinesisShardId;
    final int shardId;
    final KinesisSourceConfig config;

    /**
     * Constructor
     * @param clientId the client id
     * @param config   the kinesis source config
     * @param shardId the shard id
     */
    public KinesisShardConsumer(String clientId, KinesisSourceConfig config, int shardId) {
        this(clientId, config, shardId, createClient(clientId, config));
    }

    /**
     * Constructor, visible for testing
     * @param clientId the client id
     * @param config the Kafka source config
     * @param shardId the shard id
     * @param kinesisClient the created kinesis client
     */
    protected KinesisShardConsumer(String clientId, KinesisSourceConfig config, int shardId, KinesisClient kinesisClient) {
        this.clientId = clientId;
        this.kinesisClient = kinesisClient;
        this.shardId = shardId;
        this.config = config;

        // Get shard iterator
        DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(
            DescribeStreamRequest.builder().streamName(config.getStream()).build()
        );

        if (shardId >= describeStreamResponse.streamDescription().shards().size()) {
            throw new IllegalArgumentException("Shard id " + shardId + " does not exist in stream " + config.getStream());
        }

        String kinesisShardId = describeStreamResponse.streamDescription().shards().get(shardId).shardId();
        this.kinesisShardId = kinesisShardId;
        logger.info("kinesis consumer created for stream {} shard {}", config.getStream(), shardId);
    }

    /**
     * Create a Kafka consumer. visible for testing
     * @param clientId the client id
     * @param config the Kafka source config
     * @return the Kafka consumer
     */
    protected static KinesisClient createClient(String clientId, KinesisSourceConfig config) {

        KinesisClientBuilder kinesisClientBuilder = KinesisClient.builder()
            .region(Region.of(config.getRegion()))
            // TODO: better security config
            .credentialsProvider(StaticCredentialsProvider.create(create(config.getAccessKey(), config.getSecretKey())));

        if (config.getEndpointOverride() != null && !config.getEndpointOverride().isEmpty()) {
            try {
                kinesisClientBuilder = kinesisClientBuilder.endpointOverride(new URI(config.getEndpointOverride()));
            } catch (URISyntaxException e) {
                throw new RuntimeException("Invalid endpoint override: " + config.getEndpointOverride(), e);
            }
        }

        return kinesisClientBuilder.build();
    }

    @Override
    public List<ReadResult<SequenceNumber, KinesisMessage>> readNext(
        SequenceNumber sequenceNumber,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        List<ReadResult<SequenceNumber, KinesisMessage>> records = fetch(
            sequenceNumber.getSequenceNumber(),
            includeStart,
            maxMessages,
            timeoutMillis
        );
        return records;
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return getSequenceNumber(ShardIteratorType.TRIM_HORIZON, null, 0);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        return getSequenceNumber(ShardIteratorType.LATEST, null, 0);
    }

    private List<Record> fetchRecords(ShardIteratorType shardIteratorType, String startingSequenceNumber, long timestampMillis, int limit) {
        // Get a shard iterator AFTER the given sequence number
        GetShardIteratorRequest.Builder builder = GetShardIteratorRequest.builder()
            .streamName(config.getStream())
            .shardId(kinesisShardId)
            .shardIteratorType(shardIteratorType);

        if (startingSequenceNumber != null) {
            builder = builder.startingSequenceNumber(startingSequenceNumber);
        }

        if (timestampMillis != 0) {
            builder = builder.timestamp(Instant.ofEpochMilli(timestampMillis));
        }

        GetShardIteratorRequest shardIteratorRequest = builder.build();

        GetShardIteratorResponse shardIteratorResponse = kinesisClient.getShardIterator(shardIteratorRequest);
        String shardIterator = shardIteratorResponse.shardIterator();

        if (shardIterator == null) {
            return new ArrayList<>();
        }

        // Fetch the next records
        GetRecordsRequest recordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).limit(limit).build();

        GetRecordsResponse recordsResponse = kinesisClient.getRecords(recordsRequest);
        List<Record> records = recordsResponse.records();
        return records;
    }

    private SequenceNumber getSequenceNumber(ShardIteratorType shardIteratorType, String startingSequenceNumber, long timestampMillis) {
        List<Record> records = fetchRecords(shardIteratorType, startingSequenceNumber, timestampMillis, 1);

        if (!records.isEmpty()) {
            Record nextRecord = records.get(0);
            return new SequenceNumber(nextRecord.sequenceNumber());
        } else {
            return NON_EXISTING_SEQUENCE_NUMBER;
        }
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        // TODO: support auto config
        return getSequenceNumber(ShardIteratorType.AT_TIMESTAMP, null, timestampMillis);
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return new SequenceNumber(offset);
    }

    private synchronized List<ReadResult<SequenceNumber, KinesisMessage>> fetch(
        String sequenceNumber,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) {

        // Prepare the get records request with the shardIterator
        long limit = Math.min(maxMessages, 10000); // kinesis supports 10000 as upper limit

        ShardIteratorType iteratorType = includeStart ? ShardIteratorType.AT_SEQUENCE_NUMBER : ShardIteratorType.AFTER_SEQUENCE_NUMBER;

        List<Record> records = fetchRecords(iteratorType, sequenceNumber, 0, (int) limit);

        List<ReadResult<SequenceNumber, KinesisMessage>> results = new ArrayList<>();

        for (Record record : records) {
            SequenceNumber sequenceNumber1 = new SequenceNumber(record.sequenceNumber());
            KinesisMessage message = new KinesisMessage(record.data().asByteArray());
            results.add(new ReadResult<>(sequenceNumber1, message));
        }

        return results;
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    @Override
    public void close() throws IOException {
        if (kinesisClient != null) {
            kinesisClient.close();
        }
    }

    /**
     * Get the client id
     * @return the client id
     */
    public String getClientId() {
        return clientId;
    }
}
