/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.AdjustableSemaphore;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for Throttling logic.
 * It provides throttling functionality over multiple keys.
 * It provides the functionality of enable/disable throttling using enableThrottling variable.
 *
 * @param <T> the type of key on which we want to do throttling.
 */
public class Throttler<T> {
    protected ConcurrentMap<T, AdjustableSemaphore> semaphores = new ConcurrentHashMap<T, AdjustableSemaphore>();

    /**
     * Method to acquire permits for a key type.
     * It will return true if permits can be acquired within threshold limits.
     *
     * If threshold is not set for key then also it will return True.
     *
     * @param type Key for which we want to acquire permits.
     * @param permits Number of permits to acquire.
     * @return boolean representing was it able to acquire the permits or not.
     */
    public boolean acquire(final T type, final int permits) {
        assert permits > 0;
        AdjustableSemaphore semaphore = semaphores.get(type);
        if (Objects.nonNull(semaphore)) {
            return semaphore.tryAcquire(permits);
        }
        return true;
    }

    /**
     * Release the given permits for given type.
     *
     * @param type key for which we want to release permits.
     * @param permits number of permits to release.
     */
    public void release(final T type, final int permits) {
        assert permits > 0;
        AdjustableSemaphore semaphore = semaphores.get(type);
        if (Objects.nonNull(semaphore)) {
            semaphore.release(permits);
            assert semaphore.availablePermits() <= semaphore.getMaxPermits();
        }
    }

    /**
     * Update the Threshold for throttling for given type.
     *
     * @param key Key for which we want to update limit.
     * @param newLimit Updated limit.
     */
    public synchronized void updateThrottlingLimit(final T key, final Integer newLimit) {
        assert newLimit >= 0;
        if (semaphores.containsKey(key)) {
            semaphores.get(key).setMaxPermits(newLimit);
        } else {
            semaphores.put(key, new AdjustableSemaphore(newLimit, true));
        }
    }

    /**
     * Remove the threshold for given key.
     * Throttler will no longer do throttling for given key.
     *
     * @param key Key for which we want to remove throttling.
     */
    public synchronized void removeThrottlingLimit(final T key) {
        assert semaphores.containsKey(key);
        semaphores.remove(key);
    }

    public Integer getThrottlingLimit(final T key) {
        if (semaphores.containsKey(key)) {
            return semaphores.get(key).getMaxPermits();
        }
        return null;
    }
}
