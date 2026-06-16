/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import com.carrotsearch.randomizedtesting.ThreadFilter;

import java.net.http.HttpClient;

/**
 * The {@link HttpClient} creates own ASYNC pool based of {@code ForkJoin.commonPool()} which
 * is impossible to supress or override.
 */
public final class HttpClientThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("ForkJoinPool.commonPool-");
    }
}
