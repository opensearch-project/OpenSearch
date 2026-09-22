/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

/**
 * A minimal locking contract for objects managed by a {@link LockableConcurrentQueue}.
 *
 * @opensearch.experimental
 */
public interface Lockable {

    /**
     * Acquires the lock.
     */
    void lock();

    /**
     * Attempts to acquire the lock without blocking.
     *
     * @return {@code true} if the lock was acquired
     */
    boolean tryLock();

    /**
     * Releases the lock.
     */
    void unlock();
}
