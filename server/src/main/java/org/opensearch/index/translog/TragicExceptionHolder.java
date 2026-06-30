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
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.annotation.PublicApi;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Exception thrown if there are any massive OpenSearch failures
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TragicExceptionHolder {
    private final AtomicReference<Exception> tragedy = new AtomicReference<>();

    /**
     * Sets the tragic exception or if the tragic exception is already set adds passed exception as suppressed exception
     * @param ex tragic exception to set
     */
    public void setTragicException(Exception ex) {
        assert ex != null;
        if (tragedy.compareAndSet(null, ex)) {
            return; // first exception
        }
        final Exception tragedy = this.tragedy.get();
        // ensure no circular reference
        if (ExceptionsHelper.unwrapCausesAndSuppressed(ex, e -> e == tragedy).isPresent()) {
            assert ex == tragedy || ex instanceof AlreadyClosedException : new AssertionError("must be ACE or tragic exception", ex);
        } else {
            tragedy.addSuppressed(ex);
        }
    }

    public Exception get() {
        return tragedy.get();
    }
}
