/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * An Interface that defines Remote Store Lock Manager.
 * This will provide the functionality to acquire lock, release lock or to check if a lock is acquired on a specific
 * file in remote store.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public interface RemoteStoreLockManager {
    /**
     *
     * @param lockInfo lock info instance for which we need to acquire lock.
     * @throws IOException throws exception in case there is a problem with acquiring lock.
     */
    void acquire(LockInfo lockInfo) throws IOException;

    /**
     *
     * @param lockInfo lock info instance for which lock need to be removed.
     * @throws IOException throws exception in case there is a problem in releasing lock.
     */
    void release(LockInfo lockInfo) throws IOException;

    /**
     *
     * @param lockInfo lock info instance for which we need to check if lock is acquired.
     * @return whether a lock is acquired on the given lock info.
     * @throws IOException throws exception in case there is a problem in checking if a given file is locked or not.
     */
    Boolean isAcquired(LockInfo lockInfo) throws IOException;

    /**
     * Acquires lock on the file mentioned in originalLockInfo for acquirer mentioned in clonedLockInfo.
     * There can occur a race condition where the original file is deleted before we can use it to acquire lock for the new acquirer. Until we have a
     * fix on LockManager side, Implementors must ensure thread safety for this operation.
     * @param originalLockInfo lock info instance for original lock.
     * @param clonedLockInfo lock info instance for which lock needs to be cloned.
     * @throws IOException throws IOException if originalResource itself do not have any lock.
     */
    void cloneLock(LockInfo originalLockInfo, LockInfo clonedLockInfo) throws IOException;

    /*
    Deletes all lock related files and directories
     */
    void delete() throws IOException;
}
