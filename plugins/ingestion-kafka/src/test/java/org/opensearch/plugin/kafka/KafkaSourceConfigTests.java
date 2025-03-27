/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class KafkaSourceConfigTests extends OpenSearchTestCase {

    public void testKafkaSourceConfig() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "topic");
        params.put("bootstrap_servers", "bootstrap");
        params.put("fetch.min.bytes", 30000);
        params.put("enable.auto.commit", false);

        KafkaSourceConfig config = new KafkaSourceConfig(params);

        Assert.assertEquals("The topic should be correctly initialized and returned", "topic", config.getTopic());
        Assert.assertEquals(
            "The bootstrap servers should be correctly initialized and returned",
            "bootstrap",
            config.getBootstrapServers()
        );
        Assert.assertEquals("Incorrect fetch.min.bytes", 30000, config.getConsumerConfigurations().get("fetch.min.bytes"));
        Assert.assertEquals("Incorrect enable.auto.commit", false, config.getConsumerConfigurations().get("enable.auto.commit"));
    }
}
