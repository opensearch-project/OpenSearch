/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.http;

import java.util.List;
import java.util.Map;

/**
 * Represents the Http Header.
 */
public class HttpHeader {
    private final Map<String, List<String>> header;

    /**
     * Constructor
     * @param header header map.
     */
    public HttpHeader(Map<String, List<String>> header) {
        this.header = header;
    }

    /**
     * Returns header map.
     * @return header.
     */
    public Map<String, List<String>> getHeader() {
        return header;
    }
}
