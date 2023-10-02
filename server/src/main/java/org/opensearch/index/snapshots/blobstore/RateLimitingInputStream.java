/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.snapshots.blobstore;

import org.apache.lucene.store.RateLimiter;
import org.opensearch.common.StreamLimiter;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * Rate limiting wrapper for InputStream
 *
 * @opensearch.internal
 */
public class RateLimitingInputStream extends FilterInputStream {

    private final StreamLimiter streamLimiter;

    public RateLimitingInputStream(InputStream delegate, Supplier<RateLimiter> rateLimiterSupplier, StreamLimiter.Listener listener) {
        super(delegate);
        this.streamLimiter = new StreamLimiter(rateLimiterSupplier, listener);
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        streamLimiter.maybePause(1);
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            streamLimiter.maybePause(n);
        }
        return n;
    }
}
