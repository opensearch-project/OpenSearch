/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import java.io.IOException;

/**
 * An Interface that defines Remote Store Lock Manager.
 * This will provide the functionality to acquire lock, release lock or to check if a lock is acquired on a specific
 * file in remote store.
 * @opensearch.internal
 */
public interface RemoteStoreLockManager {
    /**
     * Acquire lock on given identifier and acquirer ID
     * @param lockInfo lock info instance for which we need to acquire lock.
     * @throws IOException throws exception in case there is a problem with acquiring lock.
     */
    void acquireLock(LockInfo lockInfo) throws IOException;

    /**
     * Release lock on given identifier and acquirer ID
     * @param lockInfo lock info instance for which lock need to be removed.
     * @throws IOException throws exception in case there is a problem in releasing lock.
     */
    void releaseLock(LockInfo lockInfo) throws IOException;

    /**
     * Checks if a lock is acquired on given identifier and acquirer ID
     * @param lockInfo lock info instance for which we need to check if lock is acquired.
     * @return whether a lock is acquired on the given lock info.
     * @throws IOException throws exception in case there is a problem in checking if a given file is locked or not.
     */
    Boolean isLockAcquired(LockInfo lockInfo) throws IOException;

    /**
     * Checks if any lock is acquired on given identifier
     * @param lockIdentifier lock identifier
     * @return whether a lock is acquired on the given lock info.
     * @throws IOException throws exception in case there is a problem in checking if a given file is locked or not.
     */
    Boolean isLockAcquired(String lockIdentifier) throws IOException;
}
