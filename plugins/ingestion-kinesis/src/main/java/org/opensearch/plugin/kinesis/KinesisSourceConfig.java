/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Booleans;
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
    private final String PROP_ENABLE_FANOUT = "enable_fanout";
    private final String PROP_FANOUT_CONSUMER_ARN = "fanout_consumer_arn";
    private final String PROP_FANOUT_CONSUMER_NAME = "fanout_consumer_name";

    private final String region;
    private final String stream;
    private final String accessKey;
    private final String secretKey;
    private final String endpointOverride;
    private final boolean fanoutEnabled;
    private final String fanoutConsumerArn;
    private final String fanoutConsumerName;

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
        this.fanoutEnabled = readBooleanParam(params, PROP_ENABLE_FANOUT, false);
        // When fan-out is enabled, either the ARN of a pre-registered consumer (used as-is) or a consumer name
        // (registered automatically if it does not already exist) must be provided.
        this.fanoutConsumerArn = ConfigurationUtils.readStringProperty(params, PROP_FANOUT_CONSUMER_ARN, "");
        this.fanoutConsumerName = ConfigurationUtils.readStringProperty(params, PROP_FANOUT_CONSUMER_NAME, "");
        if (fanoutEnabled && fanoutConsumerArn.isEmpty() && fanoutConsumerName.isEmpty()) {
            throw new OpenSearchParseException(
                "["
                    + PROP_FANOUT_CONSUMER_ARN
                    + "] or ["
                    + PROP_FANOUT_CONSUMER_NAME
                    + "] is required when ["
                    + PROP_ENABLE_FANOUT
                    + "] is enabled"
            );
        }
    }

    /**
     * Reads a boolean ingestion-source parameter. Ingestion source params come from index settings and are
     * delivered as strings (e.g. "true"), so {@link ConfigurationUtils#readBooleanProperty} (which requires an
     * actual Boolean) cannot be used directly. This accepts both a String ("true"/"false") and a Boolean.
     */
    private static boolean readBooleanParam(Map<String, Object> params, String propertyName, boolean defaultValue) {
        Object value = params.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Booleans.parseBoolean(value.toString(), defaultValue);
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

    /**
     * Whether enhanced fan-out (SubscribeToShard) is enabled
     * @return true if enhanced fan-out is enabled
     */
    public boolean isFanoutEnabled() {
        return fanoutEnabled;
    }

    /**
     * Get the consumer ARN of the registered enhanced fan-out consumer
     * @return the fan-out consumer ARN
     */
    public String getFanoutConsumerArn() {
        return fanoutConsumerArn;
    }

    /**
     * Get the name of the enhanced fan-out consumer to register (or reuse) when no ARN is provided
     * @return the fan-out consumer name
     */
    public String getFanoutConsumerName() {
        return fanoutConsumerName;
    }

}
