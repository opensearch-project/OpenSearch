/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.bulk;

import org.opensearch.common.annotation.PublicApi;

/**
 * An enum for batch ingestion option.
 */
@PublicApi(since = "2.14.0")
public enum BatchIngestionOption {
    NONE("disabled"),
    ENABLED("enabled"),
    PARALLEL("parallel");

    private final String value;

    public String getValue() {
        return this.value;
    }

    BatchIngestionOption(String value) {
        this.value = value;
    }

    static BatchIngestionOption from(String value) {
        for (BatchIngestionOption option : values()) {
            if (option.getValue().equals(value)) {
                return option;
            }
        }
        if ("".equals(value)) {
            return NONE;
        }
        throw new IllegalArgumentException("Unknown value for batch ingestion option: [" + value + "].");
    }
}
