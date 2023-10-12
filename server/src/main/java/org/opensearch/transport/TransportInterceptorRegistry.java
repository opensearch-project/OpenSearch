/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to register transport interceptors
 */
public class TransportInterceptorRegistry {

    /**
     * List of the transport interceptors
     */
    List<TransportInterceptor> transportInterceptors;

    public TransportInterceptorRegistry() {
        this.transportInterceptors = new ArrayList<>();
    }

    /**
     *
     * @param transportInterceptor new transport interceptor object
     */
    public void addTransportInterceptor(TransportInterceptor transportInterceptor) {
        this.transportInterceptors.add(transportInterceptor);
    }

    /**
     *
     * @return list of all transport interceptors
     */
    public List<TransportInterceptor> getTransportInterceptors() {
        return this.transportInterceptors;
    }
}
