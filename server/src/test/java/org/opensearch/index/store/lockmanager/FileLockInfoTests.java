/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.NoSuchFileException;

public class FileLockInfoTests extends OpenSearchTestCase {
    String testMetadata = "testMetadata";
    String testAcquirerId = "testAcquirerId";
    String testAcquirerId2 = "ZxZ4Wh89SXyEPmSYAHrIrQ";
    String testAcquirerId3 = "ZxZ4Wh89SXyEPmSYAHrItS";
    String testMetadata1 = "metadata__9223372036854775806__9223372036854775803__9223372036854775790"
        + "__9223372036854775800___Hf3Dbw2QQagfGLlVBOUrg__9223370340398865071__1";

    String oldLock = testMetadata1 + RemoteStoreLockManagerUtils.V1_LOCK_SEPARATOR + testAcquirerId2
        + RemoteStoreLockManagerUtils.V1_LOCK_FILE_EXTENSION;
    String newLock = testMetadata1 + RemoteStoreLockManagerUtils.SEPARATOR + testAcquirerId3
        + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION;

    public void testGenerateLockName() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).withAcquirerId(testAcquirerId).build();
        assertEquals(fileLockInfo.generateLockName(), FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId));

        // validate that lock generated will be the new version lock
        fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata1).withAcquirerId(testAcquirerId3).build();
        assertEquals(fileLockInfo.generateLockName(), newLock);

    }

    public void testGenerateLockNameFailureCase1() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).build();
        assertThrows(IllegalArgumentException.class, fileLockInfo::generateLockName);
    }

    public void testGenerateLockNameFailureCase2() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId).build();
        assertThrows(IllegalArgumentException.class, fileLockInfo::generateLockName);
    }

    public void testGetLockPrefix() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).build();
        assertEquals(fileLockInfo.getLockPrefix(), testMetadata + RemoteStoreLockManagerUtils.SEPARATOR);
    }

    public void testGetLockPrefixFailureCase() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId).build();
        assertThrows(IllegalArgumentException.class, fileLockInfo::getLockPrefix);
    }

    public void testGetLocksForAcquirer() throws NoSuchFileException {

        String[] locks = new String[] {
            FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId),
            FileLockInfo.LockFileUtils.generateLockName(testMetadata, "acquirerId2"),
            oldLock,
            newLock };
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId).build();
        assertEquals(fileLockInfo.getLockForAcquirer(locks), FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId));

        // validate old lock
        fileLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId2).build();
        assertEquals(fileLockInfo.getLockForAcquirer(locks), oldLock);

        // validate new lock
        fileLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId3).build();
        assertEquals(fileLockInfo.getLockForAcquirer(locks), newLock);
    }

}
