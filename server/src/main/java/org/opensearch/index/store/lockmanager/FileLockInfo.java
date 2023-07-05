/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.core.common.Strings;

/**
 * A Class that defines Info about Remote Store File Lock.
 * This is used to provide Remote Store Lock Information and some utility methods for the Lock file.
 * @opensearch.internal
 */
public class FileLockInfo implements LockInfo {
    private final String fileToLock;
    private final String acquirerId;

    public FileLockInfo(String fileToLock, String acquirerId) {
        if (Strings.isNullOrEmpty(fileToLock) || Strings.isNullOrEmpty(acquirerId)) {
            throw new IllegalArgumentException("Both the arguments should be non-empty");
        }
        this.fileToLock = fileToLock;
        this.acquirerId = acquirerId;
    }

    public String getAcquirerId() {
        return acquirerId;
    }

    public String getFileToLock() {
        return fileToLock;
    }

    @Override
    public String generateLockName() {
        return String.join(RemoteStoreLockManagerUtils.SEPARATOR, fileToLock, acquirerId) + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION;
    }

    public static String getAcquirerIdFromLock(String lockName) {
        String[] lockNameTokens = lockName.split(RemoteStoreLockManagerUtils.SEPARATOR);

        if (lockNameTokens.length != 2) {
            throw new IllegalArgumentException("Provided Lock Name " + lockName + " is not Valid.");
        }
        return lockNameTokens[1].replace(RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION, "");
    }
}
