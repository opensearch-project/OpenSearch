/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class FileLockInfoTests extends OpenSearchTestCase {
    String testMetadata = "testMetadata";
    String testAcquirerId = "testAcquirerId";

    public void testGenerateLockName() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).withAcquirerId(testAcquirerId).build();
        assertEquals(fileLockInfo.generateLockName(), FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId));
    }

    public void testGenerateLockNameFailureCaseNoFile() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock("").withAcquirerId(testAcquirerId).build();
        assertThrows(IllegalArgumentException.class, fileLockInfo::generateLockName);
    }

    public void testGenerateLockNameFailureCaseNoAcquirer() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).withAcquirerId("").build();
        assertThrows(IllegalArgumentException.class, fileLockInfo::generateLockName);
    }

    public void testGetLockPrefix() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).withAcquirerId(testAcquirerId).build();
        assertEquals(fileLockInfo.getLockPrefix(), testMetadata + RemoteStoreLockManagerUtils.SEPARATOR);
    }

    public void testGetLockPrefixFailureCaseNoFile() {
        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock("").withAcquirerId(testAcquirerId).build();
        assertThrows(IllegalArgumentException.class, fileLockInfo::getLockPrefix);
    }

    public void testGetLocksForAcquirer() {
        String[] locks = new String[] {
            FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId),
            FileLockInfo.LockFileUtils.generateLockName(testMetadata, "acquirerId2") };

        FileLockInfo fileLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).withAcquirerId(testAcquirerId).build();

        assertEquals(
            fileLockInfo.getLocksForAcquirer(locks),
            List.of(FileLockInfo.LockFileUtils.generateLockName(testMetadata, testAcquirerId))
        );
    }

}
