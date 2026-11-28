/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KinesisSourceConfigTests extends OpenSearchTestCase {
    public void testConstructorAndGetters() {
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("endpoint_override", "testEndpoint");

        KinesisSourceConfig config = new KinesisSourceConfig(params);

        Assert.assertEquals("Region should be correctly initialized and returned", "us-west-2", config.getRegion());
        Assert.assertEquals("Stream should be correctly initialized and returned", "testStream", config.getStream());
        Assert.assertEquals("Access key should be correctly initialized and returned", "testAccessKey", config.getAccessKey());
        Assert.assertEquals("Secret key should be correctly initialized and returned", "testSecretKey", config.getSecretKey());
        Assert.assertEquals("Endpoint override should be correctly initialized and returned", "testEndpoint", config.getEndpointOverride());
        Assert.assertFalse("Fan-out should be disabled by default", config.isFanoutEnabled());
        Assert.assertEquals("Fan-out consumer ARN should default to empty", "", config.getFanoutConsumerArn());
    }

    public void testFanoutConfig() {
        // Ingestion source params arrive from index settings as strings (e.g. "true"), not as a Boolean.
        // This mirrors the production type; readBooleanProperty would reject a String here.
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("enable_fanout", "true");
        params.put("fanout_consumer_arn", "arn:aws:kinesis:us-west-2:123456789012:stream/testStream/consumer/c:1");

        KinesisSourceConfig config = new KinesisSourceConfig(params);

        Assert.assertTrue("Fan-out should be enabled", config.isFanoutEnabled());
        Assert.assertEquals(
            "Fan-out consumer ARN should be correctly initialized and returned",
            "arn:aws:kinesis:us-west-2:123456789012:stream/testStream/consumer/c:1",
            config.getFanoutConsumerArn()
        );
    }

    public void testFanoutConfigAcceptsBooleanValue() {
        // Also accept an actual Boolean, for programmatic callers.
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("enable_fanout", true);
        params.put("fanout_consumer_arn", "arn:aws:kinesis:us-west-2:123456789012:stream/testStream/consumer/c:1");

        KinesisSourceConfig config = new KinesisSourceConfig(params);
        Assert.assertTrue("Fan-out should be enabled from a Boolean value", config.isFanoutEnabled());
    }

    public void testFanoutDisabledByStringFalse() {
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("enable_fanout", "false");

        KinesisSourceConfig config = new KinesisSourceConfig(params);
        Assert.assertFalse("Fan-out should be disabled for string \"false\"", config.isFanoutEnabled());
    }

    public void testFanoutConfigWithConsumerName() {
        // No ARN, but a consumer name is provided, so the consumer is registered/reused automatically.
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("enable_fanout", "true");
        params.put("fanout_consumer_name", "my-consumer");

        KinesisSourceConfig config = new KinesisSourceConfig(params);

        Assert.assertTrue("Fan-out should be enabled", config.isFanoutEnabled());
        Assert.assertEquals("Consumer name should be returned", "my-consumer", config.getFanoutConsumerName());
        Assert.assertEquals("Consumer ARN should default to empty", "", config.getFanoutConsumerArn());
    }

    public void testFanoutConfigRequiresArnOrName() {
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("enable_fanout", "true");

        try {
            new KinesisSourceConfig(params);
            Assert.fail("Constructor should throw an exception when fan-out is enabled without an ARN or a name");
        } catch (OpenSearchParseException e) {
            Assert.assertEquals(
                "[fanout_consumer_arn] or [fanout_consumer_name] is required when [enable_fanout] is enabled",
                e.getMessage()
            );
        }
    }

    public void testConstructorFails() {
        try {
            new KinesisSourceConfig(null);
            Assert.fail("Constructor should throw an exception when params is null");
        } catch (NullPointerException e) {
            Assert.assertEquals("Cannot invoke \"java.util.Map.get(Object)\" because \"configuration\" is null", e.getMessage());
        }

        try {
            new KinesisSourceConfig(Collections.emptyMap());
            Assert.fail("Constructor should throw an exception when params is empty");
        } catch (OpenSearchParseException e) {
            Assert.assertEquals("[region] required property is missing", e.getMessage());
        }
    }
}
