/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * A Class that implements Remote Store Lock Manager by creating lock files for the remote store files that needs to
 * be locked.
 * It uses {@code LockFileInfo} instance to get the information about the lock file on which operations need to
 * be executed.
 *
 * @opensearch.internal
 */
public class RemoteStoreMetadataLockManager implements RemoteStoreLockManager {
    private static final Logger logger = LogManager.getLogger(RemoteStoreMetadataLockManager.class);
    private final RemoteBufferedOutputDirectory lockDirectory;

    public RemoteStoreMetadataLockManager(RemoteBufferedOutputDirectory lockDirectory) {
        this.lockDirectory = lockDirectory;
    }

    /**
     * Acquires lock on the file mentioned in LockInfo Instance.
     * @param lockInfo File Lock Info instance for which we need to acquire lock.
     * @throws IOException in case there is some failure while acquiring lock.
     */
    @Override
    public void acquire(LockInfo lockInfo) throws IOException {
        assert lockInfo instanceof FileLockInfo : "lockInfo should be instance of FileLockInfo";
        IndexOutput indexOutput = lockDirectory.createOutput(lockInfo.generateLockName(), IOContext.DEFAULT);
        indexOutput.close();
    }

    /**
     * Releases Locks acquired by a given acquirer which is passed in LockInfo Instance.
     * Right now this method is only used to release locks for a given acquirer,
     * This can be extended in future to handle other cases as well, like:
     * - release lock for given fileToLock and AcquirerId
     * - release all locks for given fileToLock
     * @param lockInfo File Lock Info instance for which lock need to be removed.
     * @throws IOException in case there is some failure in releasing locks.
     */
    @Override
    public void release(LockInfo lockInfo) throws IOException {
        assert lockInfo instanceof FileLockInfo : "lockInfo should be instance of FileLockInfo";
        String[] lockFiles = lockDirectory.listAll();

        // ideally there should be only one lock per acquirer, but just to handle any stale locks,
        // we try to release all the locks for the acquirer.
        List<String> locksToRelease = ((FileLockInfo) lockInfo).getLocksForAcquirer(lockFiles);
        if (locksToRelease.size() > 1) {
            logger.warn(locksToRelease.size() + " locks found for acquirer " + ((FileLockInfo) lockInfo).getAcquirerId());
        }
        for (String lock : locksToRelease) {
            lockDirectory.deleteFile(lock);
        }
    }

    /**
     * Checks whether a given file have any lock on it or not.
     * @param lockInfo File Lock Info instance for which we need to check if lock is acquired.
     * @return true if lock is acquired on a file, else false.
     * @throws IOException in case there is some failure in checking locks for a file.
     */
    @Override
    public Boolean isAcquired(LockInfo lockInfo) throws IOException {
        assert lockInfo instanceof FileLockInfo : "lockInfo should be instance of FileLockInfo";
        Collection<String> lockFiles = lockDirectory.listFilesByPrefix(((FileLockInfo) lockInfo).getLockPrefix());
        return !lockFiles.isEmpty();
    }

    public void delete() throws IOException {
        lockDirectory.delete();
    }
}
