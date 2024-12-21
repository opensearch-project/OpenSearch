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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Class that implements Remote Store Lock Manager by creating lock files for the remote store files that needs to
 * be locked.
 * It uses {@code LockFileInfo} instance to get the information about the lock file on which operations need to
 * be executed.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
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
     * If the lock file doesn't exist for the acquirer, release will be a no-op.
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
        String[] lockFiles = getLockFiles(lockDirectory.listAll());
        try {
            String lockToRelease = ((FileLockInfo) lockInfo).getLockForAcquirer(lockFiles);
            lockDirectory.deleteFile(lockToRelease);
        } catch (NoSuchFileException e) {
            // Ignoring if the file to be deleted is not present.
            logger.info("No lock file found for acquirerId: {}", ((FileLockInfo) lockInfo).getAcquirerId());
        }
    }

    public String fetchLockedMetadataFile(String filenamePrefix, String acquirerId) throws IOException {
        Collection<String> lockFiles = lockDirectory.listFilesByPrefix(filenamePrefix);
        List<String> lockFilesForAcquirer = lockFiles.stream()
            .filter(lockFile -> acquirerId.equals(FileLockInfo.LockFileUtils.getAcquirerIdFromLock(lockFile)))
            .map(FileLockInfo.LockFileUtils::getFileToLockNameFromLock)
            .collect(Collectors.toList());
        if (lockFilesForAcquirer.size() == 0) {
            throw new FileNotFoundException("No lock file found for prefix: " + filenamePrefix + " and acquirerId: " + acquirerId);
        }
        assert lockFilesForAcquirer.size() == 1;
        return lockFilesForAcquirer.get(0);
    }

    public Set<String> fetchLockedMetadataFiles(String filenamePrefix) throws IOException {
        Collection<String> lockFiles = lockDirectory.listFilesByPrefix(filenamePrefix);
        return lockFiles.stream().map(FileLockInfo.LockFileUtils::getFileToLockNameFromLock).collect(Collectors.toSet());
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

    /**
     * Acquires lock on the file mentioned in originalLockInfo for acquirer mentioned in clonedLockInfo.
     * Snapshot layer enforces thread safety by having checks in place to ensure that the source snapshot is not being deleted before proceeding
     * with the clone operation. Hence, the original lock file would always be present while acquiring the lock for cloned snapshot.
     * @param originalLockInfo lock info instance for original lock.
     * @param clonedLockInfo lock info instance for which lock needs to be cloned.
     * @throws IOException throws IOException if originalResource itself do not have any lock.
     */
    @Override
    public void cloneLock(LockInfo originalLockInfo, LockInfo clonedLockInfo) throws IOException {
        assert originalLockInfo instanceof FileLockInfo : "originalLockInfo should be instance of FileLockInfo";
        assert clonedLockInfo instanceof FileLockInfo : "clonedLockInfo should be instance of FileLockInfo";
        String originalResourceId = Objects.requireNonNull(((FileLockInfo) originalLockInfo).getAcquirerId());
        String clonedResourceId = Objects.requireNonNull(((FileLockInfo) clonedLockInfo).getAcquirerId());
        assert originalResourceId != null && clonedResourceId != null : "provided resourceIds should not be null";
        String[] lockFiles = getLockFiles(lockDirectory.listAll());
        String lockNameForAcquirer = ((FileLockInfo) originalLockInfo).getLockForAcquirer(lockFiles);
        String fileToLockName = FileLockInfo.LockFileUtils.getFileToLockNameFromLock(lockNameForAcquirer);
        acquire(FileLockInfo.getLockInfoBuilder().withFileToLock(fileToLockName).withAcquirerId(clonedResourceId).build());
    }

    public void delete() throws IOException {
        lockDirectory.delete();
    }

    private String[] getLockFiles(String[] lockDirectoryContents) throws IOException {
        if (lockDirectoryContents == null || lockDirectoryContents.length == 0) {
            return new String[0];
        }
        // filtering lock files from lock directory contents.
        // this is a good to have check, there is no known prod scenarios where this can happen
        // however, during tests sometimes while creating local file directory lucene adds extraFS files.
        return Arrays.stream(lockDirectory.listAll())
            .filter(
                file -> file.endsWith(RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION)
                    || file.endsWith(RemoteStoreLockManagerUtils.PRE_OS210_LOCK_FILE_EXTENSION)
            )
            .toArray(String[]::new);
    }
}
