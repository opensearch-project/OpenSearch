/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.core.util.ConfigurationUtils;

import java.util.Map;

/**
 * Class encapsulating the configuration of a Kafka source.
 */
public class KinesisSourceConfig {
    private final String PROP_REGION = "region";
    private final String PROP_STREAM = "stream";
    private final String PROP_ACCESS_KEY = "access_key";
    private final String PROP_SECRET_KEY = "secret_key";
    private final String PROP_ENDPOINT_OVERRIDE = "endpoint_override";

    private final String region;
    private final String stream;
    private final String accessKey;
    private final String secretKey;
    private final String endpointOverride;

    /**
     * Constructor
     * @param params the configuration parameters
     */
    public KinesisSourceConfig(Map<String, Object> params) {
        this.region = ConfigurationUtils.readStringProperty(params, PROP_REGION);
        this.stream = ConfigurationUtils.readStringProperty(params, PROP_STREAM);
        this.accessKey = ConfigurationUtils.readStringProperty(params, PROP_ACCESS_KEY);
        this.secretKey = ConfigurationUtils.readStringProperty(params, PROP_SECRET_KEY);
        this.endpointOverride = ConfigurationUtils.readStringProperty(params, PROP_ENDPOINT_OVERRIDE, "");
    }

    /**
     * Get the stream name
     * @return the topic name
     */
    public String getStream() {
        return stream;
    }

    /**
     * Get the region
     * @return the region
     */
    public String getRegion() {
        return region;
    }

    /**
     * Get the access key
     * @return the access key
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Get the secret key
     * @return the secret key
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Get the endpoint override
     * @return the endpoint override
     */
    public String getEndpointOverride() {
        return endpointOverride;
    }

}
