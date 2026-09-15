/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KinesisConsumerFactoryTests extends OpenSearchTestCase {
    public void testConstructor() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        Assert.assertNotNull("Factory should be created", factory);
    }

    public void testCreateShardConsumerWithNullMetadata() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        expectThrows(NullPointerException.class, () -> factory.createShardConsumer("test-client", 0, (IndexMetadata) null));
    }

    public void testParsePointerFromString() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        SequenceNumber sequenceNumber = factory.parsePointerFromString("12345");

        Assert.assertNotNull("Sequence number should be parsed", sequenceNumber);
        Assert.assertEquals("Sequence number should be correctly parsed", "12345", sequenceNumber.getSequenceNumber());
    }

    private static KinesisSourceConfig fanoutConfig(String arn, String name) {
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("enable_fanout", "true");
        if (arn != null) {
            params.put("fanout_consumer_arn", arn);
        }
        if (name != null) {
            params.put("fanout_consumer_name", name);
        }
        return new KinesisSourceConfig(params);
    }

    public void testResolveConsumerArnUsesConfiguredArnWithoutRegistering() {
        AtomicInteger registrations = new AtomicInteger();
        KinesisConsumerFactory factory = new KinesisConsumerFactory() {
            @Override
            protected String registerConsumer(String clientId, KinesisSourceConfig config) {
                registrations.incrementAndGet();
                return "registered-arn";
            }
        };

        String arn = factory.resolveConsumerArn("client", fanoutConfig("configured-arn", null));

        Assert.assertEquals("configured-arn", arn);
        Assert.assertEquals("A configured ARN must not trigger registration", 0, registrations.get());
    }

    public void testResolveConsumerArnRegistersOncePerStreamAndCaches() {
        AtomicInteger registrations = new AtomicInteger();
        KinesisConsumerFactory factory = new KinesisConsumerFactory() {
            @Override
            protected String registerConsumer(String clientId, KinesisSourceConfig config) {
                registrations.incrementAndGet();
                return "registered-arn";
            }
        };

        // two shards of the same stream+name resolve the ARN; registration must happen only once (cached)
        String first = factory.resolveConsumerArn("client", fanoutConfig(null, "my-consumer"));
        String second = factory.resolveConsumerArn("client", fanoutConfig(null, "my-consumer"));

        Assert.assertEquals("registered-arn", first);
        Assert.assertEquals("registered-arn", second);
        Assert.assertEquals("Registration must be cached per stream+name", 1, registrations.get());
    }
}
