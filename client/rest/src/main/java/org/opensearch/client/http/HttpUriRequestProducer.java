/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.http;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.net.URIAuthority;
import org.apache.hc.core5.util.Args;
import org.opensearch.client.nio.HttpEntityAsyncEntityProducer;

/**
 * The producer of the {@link HttpUriRequestBase} instances associated with a particular {@link HttpHost}
 */
public class HttpUriRequestProducer extends BasicRequestProducer {
    private final HttpUriRequestBase request;

    HttpUriRequestProducer(final HttpUriRequestBase request, final AsyncEntityProducer entityProducer) {
        super(request, entityProducer);
        this.request = request;
    }

    /**
     * Get the produced {@link HttpUriRequestBase} instance
     * @return produced {@link HttpUriRequestBase} instance
     */
    public HttpUriRequestBase getRequest() {
        return request;
    }

    /**
     * Create new request producer for {@link HttpUriRequestBase} instance and {@link HttpHost}
     * @param request {@link HttpUriRequestBase} instance
     * @param host {@link HttpHost} instance
     * @return new request producer
     */
    public static HttpUriRequestProducer create(final HttpUriRequestBase request, final HttpHost host) {
        Args.notNull(request, "Request");
        Args.notNull(host, "HttpHost");

        // TODO: Should we copy request here instead of modifying in place?
        request.setAuthority(new URIAuthority(host));
        request.setScheme(host.getSchemeName());

        final HttpEntity entity = request.getEntity();
        AsyncEntityProducer entityProducer = null;

        if (entity != null) {
            entityProducer = new HttpEntityAsyncEntityProducer(entity);
        }

        return new HttpUriRequestProducer(request, entityProducer);
    }

}
