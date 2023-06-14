/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Class that defines Info about Remote Store File Lock.
 * This is used to provide Remote Store Lock Information and some utility methods for the Lock file.
 * @opensearch.internal
 */
public class FileLockInfo implements LockInfo {
    private String fileToLock;
    private String acquirerId;

    public String getAcquirerId() {
        return acquirerId;
    }

    public String getFileToLock() {
        return fileToLock;
    }

    private void setFileToLock(String fileName) {
        this.fileToLock = fileName;
    }

    private void setAcquirerId(String acquirerId) {
        this.acquirerId = acquirerId;
    }

    @Override
    public String generateLockName() {
        validateRequiredParameters(this);
        return LockFileUtils.generateLockName(fileToLock, acquirerId);
    }

    String getLockPrefix() {
        if (fileToLock == null || fileToLock.isBlank()) {
            throw new IllegalArgumentException("File to Lock should be provided");
        }
        return fileToLock + RemoteStoreLockManagerUtils.SEPARATOR;
    }

    String getLockForAcquirer(String[] lockFiles) throws NoSuchFileException {
        if (acquirerId == null || acquirerId.isBlank()) {
            throw new IllegalArgumentException("Acquirer ID should be provided");
        }
        List<String> locksForAcquirer = Arrays.stream(lockFiles)
            .filter(lockFile -> acquirerId.equals(LockFileUtils.getAcquirerIdFromLock(lockFile)))
            .collect(Collectors.toList());

        if (locksForAcquirer.isEmpty()) {
            throw new NoSuchFileException("No lock file found for the acquirer: " + acquirerId);
        }
        if (locksForAcquirer.size() != 1) {
            throw new IllegalStateException("Expected single lock file but found [" + locksForAcquirer.size() + "] lock files");
        }
        return locksForAcquirer.get(0);
    }

    public static LockInfoBuilder getLockInfoBuilder() {
        return new LockInfoBuilder();
    }

    private static void validateRequiredParameters(FileLockInfo fileLockInfo) {
        if (fileLockInfo.getAcquirerId() == null || fileLockInfo.getAcquirerId().isBlank()) {
            throw new IllegalArgumentException("Acquirer ID should be provided");
        }
        if (fileLockInfo.getFileToLock() == null || fileLockInfo.getFileToLock().isBlank()) {
            throw new IllegalArgumentException("File to Lock should be provided");
        }
    }

    static class LockFileUtils {
        static String generateLockName(String fileToLock, String acquirerId) {
            return String.join(RemoteStoreLockManagerUtils.SEPARATOR, fileToLock, acquirerId)
                + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION;
        }

        public static String getFileToLockNameFromLock(String lockName) {
            String[] lockNameTokens = lockName.split(RemoteStoreLockManagerUtils.SEPARATOR);

            if (lockNameTokens.length != 2) {
                throw new IllegalArgumentException("Provided Lock Name " + lockName + " is not Valid.");
            }
            return lockNameTokens[0];
        }

        public static String getAcquirerIdFromLock(String lockName) {
            String[] lockNameTokens = lockName.split(RemoteStoreLockManagerUtils.SEPARATOR);

            if (lockNameTokens.length != 2) {
                throw new IllegalArgumentException("Provided Lock Name " + lockName + " is not Valid.");
            }
            return lockNameTokens[1].replace(RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION, "");
        }
    }

    /**
     * A Builder Class to build an Instance of {@code FileLockInfo}
     * @opensearch.internal
     */
    public static class LockInfoBuilder implements LockInfo.LockInfoBuilder {
        private final FileLockInfo lockFileInfo;

        LockInfoBuilder() {
            this.lockFileInfo = new FileLockInfo();
        }

        public LockInfoBuilder withFileToLock(String fileToLock) {
            lockFileInfo.setFileToLock(fileToLock);
            return this;
        }

        public LockInfoBuilder withAcquirerId(String acquirerId) {
            lockFileInfo.setAcquirerId(acquirerId);
            return this;
        }

        @Override
        public FileLockInfo build() {
            if (lockFileInfo.fileToLock == null && lockFileInfo.acquirerId == null) {
                throw new IllegalStateException("Either File to Lock or AcquirerId should be provided to instantiate FileLockInfo");
            }
            return lockFileInfo;
        }
    }
}
