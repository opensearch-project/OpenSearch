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
