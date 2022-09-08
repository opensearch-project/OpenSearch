/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.http;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.util.Args;
import org.opensearch.client.nio.HttpEntityAsyncEntityProducer;

public class ClassicRequestProducer extends BasicRequestProducer {

    ClassicRequestProducer(final ClassicHttpRequest request, final AsyncEntityProducer entityProducer) {
        super(request, entityProducer);
    }

    public static ClassicRequestProducer create(final ClassicHttpRequest request) {
        Args.notNull(request, "Request");

        final HttpEntity entity = request.getEntity();
        AsyncEntityProducer entityProducer = null;

        if (entity != null) {
            entityProducer = new HttpEntityAsyncEntityProducer(entity);
        }

        return new ClassicRequestProducer(request, entityProducer);
    }

}
