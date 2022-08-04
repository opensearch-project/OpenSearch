/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
 *
 * @opensearch.internal
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
