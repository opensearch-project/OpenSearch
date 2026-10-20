/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link Lockable} wrapper around an arbitrary reference, pairing the value with
 * a {@link ReentrantLock} for use in pool-based concurrency patterns.
 *
 * <p>Used by {@link LockablePool} to track items that can be locked for exclusive
 * access (e.g., writers in the indexing pipeline) and unlocked when returned to the pool.
 *
 * @param <T> the type of the wrapped reference
 */
public class DefaultLockableHolder<T> implements Lockable {

    private final T ref;
    private final ReentrantLock lock = new ReentrantLock();

    private DefaultLockableHolder(T ref) {
        this.ref = ref;
    }

    /**
     * Creates a new holder wrapping the given reference.
     *
     * @param ref the reference to wrap
     * @param <R> the reference type
     * @return a new {@code DefaultLockableHolder} containing {@code ref}
     */
    public static <R> DefaultLockableHolder<R> of(R ref) {
        return new DefaultLockableHolder<>(ref);
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    /**
     * Returns the wrapped reference.
     *
     * @return the reference held by this holder
     */
    public T get() {
        return ref;
    }
}
