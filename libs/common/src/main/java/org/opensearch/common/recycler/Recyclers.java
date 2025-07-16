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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.recycler;

import org.opensearch.common.util.BitMixer;

import java.util.ArrayDeque;

/**
 * Utility class of recyclers.
 *
 * @opensearch.internal
 */
public enum Recyclers {
    ;

    /**
     * Return a {@link Recycler} that never recycles entries.
     */
    public static <T> Recycler<T> none(Recycler.C<T> c) {
        return new NoneRecycler<>(c);
    }

    /**
     * Return a concurrent recycler based on a deque.
     */
    public static <T> Recycler<T> concurrentDeque(Recycler.C<T> c, int limit) {
        return new ConcurrentDequeRecycler<>(c, limit);
    }

    /**
     * Return a recycler based on a deque.
     */
    public static <T> Recycler<T> deque(Recycler.C<T> c, int limit) {
        return new DequeRecycler<>(c, new ArrayDeque<>(), limit);
    }

    /**
     * Return a recycler based on a deque.
     */
    public static <T> Recycler.Factory<T> dequeFactory(final Recycler.C<T> c, final int limit) {
        return () -> deque(c, limit);
    }

    /**
     * Wrap the provided recycler so that calls to {@link Recycler#obtain()} and {@link Recycler.V#close()} are protected by
     * a lock.
     *
     * @opensearch.internal
     */
    public static <T> Recycler<T> locked(final Recycler<T> recycler) {
        return new FilterRecycler<T>() {

            private final Object lock;

            {
                this.lock = new Object();
            }

            @Override
            protected Recycler<T> getDelegate() {
                return recycler;
            }

            @Override
            public Recycler.V<T> obtain() {
                synchronized (lock) {
                    return super.obtain();
                }
            }

            @Override
            protected Recycler.V<T> wrap(final Recycler.V<T> delegate) {
                return new Recycler.V<T>() {

                    @Override
                    public void close() {
                        synchronized (lock) {
                            delegate.close();
                        }
                    }

                    @Override
                    public T v() {
                        return delegate.v();
                    }

                    @Override
                    public boolean isRecycled() {
                        return delegate.isRecycled();
                    }

                };
            }

        };
    }

    /**
     * Create a concurrent implementation that can support concurrent access from
     * <code>concurrencyLevel</code> threads with little contention.
     */
    public static <T> Recycler<T> concurrent(final Recycler.Factory<T> factory, final int concurrencyLevel) {
        if (concurrencyLevel < 1) {
            throw new IllegalArgumentException("concurrencyLevel must be >= 1");
        }
        if (concurrencyLevel == 1) {
            return locked(factory.build());
        }
        return new FilterRecycler<T>() {

            private final Recycler<T>[] recyclers;

            {
                @SuppressWarnings({ "rawtypes", "unchecked" })
                final Recycler<T>[] recyclers = new Recycler[concurrencyLevel];
                this.recyclers = recyclers;
                for (int i = 0; i < concurrencyLevel; ++i) {
                    recyclers[i] = locked(factory.build());
                }
            }

            int slot() {
                final long id = Thread.currentThread().threadId();
                // don't trust Thread.hashCode to have equiprobable low bits
                int slot = (int) BitMixer.mix64(id);
                // make positive, otherwise % may return negative numbers
                slot &= 0x7FFFFFFF;
                slot %= concurrencyLevel;
                return slot;
            }

            @Override
            protected Recycler<T> getDelegate() {
                return recyclers[slot()];
            }

        };
    }

    public static <T> Recycler<T> concurrent(final Recycler.Factory<T> factory) {
        return concurrent(factory, Runtime.getRuntime().availableProcessors());
    }
}
