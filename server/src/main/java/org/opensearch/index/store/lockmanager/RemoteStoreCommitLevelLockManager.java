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
 * An Interface that defines Commit Level Lock in Remote Store. We can lock the segment files corresponding to a given
 * primaryTerm and Commit Generation.
 *
 * @opensearch.internal
 */
public interface RemoteStoreCommitLevelLockManager {
    /**
     *
     * This method will be used to acquire lock on segment files of a specific commit.
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation Commit Generation
     * @param acquirerId Resource ID which wants to acquire lock on the commit.
     * @throws IOException in case there is a problem in acquiring lock on a commit.
     */
    void acquireLock(long primaryTerm, long generation, String acquirerId) throws IOException;

    /**
     * This method will be used to release lock on segment files of a specific commit, which got acquired by given
     * resource.
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation Commit Generation
     * @param acquirerId Resource ID for which lock needs to be released.
     * @throws IOException in case there is a problem in releasing lock on a commit.
     */
    void releaseLock(long primaryTerm, long generation, String acquirerId) throws IOException;

    /**
     * This method will be used to check if a specific commit have any lock acquired on it or not.
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation Commit Generation
     * @return true if given commit is locked, else false.
     * @throws IOException in case there is a problem in checking if a commit is locked or not.
     */
    Boolean isLockAcquired(long primaryTerm, long generation) throws IOException;
}
