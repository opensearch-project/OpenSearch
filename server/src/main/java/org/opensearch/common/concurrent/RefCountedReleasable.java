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

package org.opensearch.common.concurrent;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.AbstractRefCounted;

/**
 * Decorator class that wraps an object reference as a {@link AbstractRefCounted} instance.
 * In addition to a {@link String} name, it accepts a {@link Runnable} shutdown hook that is
 * invoked when the reference count reaches zero i.e. on {@link #closeInternal()}.
 */
public class RefCountedReleasable<T> extends AbstractRefCounted implements Releasable {

    private final T ref;
    private final Runnable shutdownRunnable;

    public RefCountedReleasable(String name, T ref, Runnable shutdownRunnable) {
        super(name);
        this.ref = ref;
        this.shutdownRunnable = shutdownRunnable;
    }

    @Override
    public void close() {
        decRef();
    }

    public T get() {
        return ref;
    }

    @Override
    protected void closeInternal() {
        shutdownRunnable.run();
    }
}
