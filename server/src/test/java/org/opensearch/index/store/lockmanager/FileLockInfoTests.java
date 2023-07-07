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

    public void testGenerateLockName() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).withAcquirerId(testAcquirerId).build();
        assertEquals(fileLockInfo.generateLockName(), FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId));
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
            FileLockInfo.LockFileUtils.generateLockName(testMetadata, "acquirerId2") };
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId).build();

        assertEquals(fileLockInfo.getLockForAcquirer(locks), FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId));
    }

}
