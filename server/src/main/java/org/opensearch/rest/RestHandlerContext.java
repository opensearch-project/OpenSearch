/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.common.util.concurrent.ThreadContext;

/**
 * Holder for information that is shared between stages of the request pipeline
 */
public class RestHandlerContext {
    private RestResponse earlyResponse;
    private ThreadContext.StoredContext contextToRestore;

    public static RestHandlerContext EMPTY = new RestHandlerContext();

    private RestHandlerContext() {}

    public RestHandlerContext(final RestResponse earlyResponse, ThreadContext.StoredContext contextToRestore) {
        this.earlyResponse = earlyResponse;
        this.contextToRestore = contextToRestore;
    }

    public boolean hasEarlyResponse() {
        return this.earlyResponse != null;
    }

    public boolean hasContextToRestore() {
        return this.contextToRestore != null;
    }

    public RestResponse getEarlyResponse() {
        return this.earlyResponse;
    }

    public ThreadContext.StoredContext getContextToRestore() {
        return contextToRestore;
    }
}
