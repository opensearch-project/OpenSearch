/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.AdjustableSemaphore;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for Throttling logic.
 * It provides throttling functionality over multiple keys.
 */
public class Throttler {
    protected ConcurrentMap<String, AdjustableSemaphore> semaphores = new ConcurrentHashMap<String, AdjustableSemaphore>();

    /**
     * Method to acquire permits for a key type.
     * It will return true if permits can be acquired within threshold limits else false.
     *
     * If Throttler is not configured for the key then it will return Optional.empty().
     * calling function need to handle this for determining the default behavior.
     *
     * @param key Key for which we want to acquire permits.
     * @param permits Number of permits to acquire.
     * @return Optional(Boolean) True/False - Throttler is configured for key and is able to acquire the permits or not
     *                           Optional.empty() - Throttler is not configured for key
     */
    public Optional<Boolean> acquire(final String key, final int permits) {
        assert permits > 0;
        AdjustableSemaphore semaphore = semaphores.get(key);
        if (semaphore != null) {
            return Optional.of(semaphore.tryAcquire(permits));
        }
        return Optional.empty();
    }

    /**
     * Release the given permits for given type.
     *
     * @param key key for which we want to release permits.
     * @param permits number of permits to release.
     */
    public void release(final String key, final int permits) {
        assert permits > 0;
        AdjustableSemaphore semaphore = semaphores.get(key);
        if (semaphore != null) {
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
    public void updateThrottlingLimit(final String key, final Integer newLimit) {
        assert newLimit >= 0;
        AdjustableSemaphore semaphore = semaphores.get(key);
        if (semaphore == null) {
            semaphore = semaphores.computeIfAbsent(key, k -> new AdjustableSemaphore(newLimit, true));
        }
        semaphore.setMaxPermits(newLimit);
    }

    /**
     * Remove the threshold for given key.
     * Throttler will no longer do throttling for given key.
     *
     * @param key Key for which we want to remove throttling.
     */
    public void removeThrottlingLimit(final String key) {
        assert semaphores.containsKey(key);
        semaphores.remove(key);
    }

    public Integer getThrottlingLimit(final String key) {
        AdjustableSemaphore semaphore = semaphores.get(key);
        if (semaphore != null) {
            return semaphore.getMaxPermits();
        }
        return null;
    }
}
