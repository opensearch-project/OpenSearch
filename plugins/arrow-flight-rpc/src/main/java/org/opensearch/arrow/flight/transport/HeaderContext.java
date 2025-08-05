/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.transport.Header;

import java.util.concurrent.ConcurrentHashMap;

class HeaderContext {
    private final ConcurrentHashMap<Long, Header> headerMap = new ConcurrentHashMap<>();

    void setHeader(long correlationId, Header header) {
        headerMap.put(correlationId, header);
    }

    Header getHeader(long correlationId) {
        return headerMap.remove(correlationId);
    }
}
