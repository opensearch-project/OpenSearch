/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.test.OpenSearchTestCase;

public class FileLockInfoTests extends OpenSearchTestCase {
    String testMetadata = "testMetadata";
    String testAcquirerId = "testAcquirerId";

    public void testFileInfoCreationFailureNoFile() {
        assertThrows(IllegalArgumentException.class, () -> new FileLockInfo(null, testAcquirerId));
        assertThrows(IllegalArgumentException.class, () -> new FileLockInfo("", testAcquirerId));
    }

    public void testFileInfoCreationFailureNoAcquirer() {
        assertThrows(IllegalArgumentException.class, () -> new FileLockInfo(testMetadata, null));
        assertThrows(IllegalArgumentException.class, () -> new FileLockInfo(testMetadata, ""));
    }

    public void testGenerateLockName() {
        FileLockInfo fileLockInfo = new FileLockInfo(testMetadata, testAcquirerId);
        assertEquals(
            fileLockInfo.generateLockName(),
            String.join(RemoteStoreLockManagerUtils.SEPARATOR, testMetadata, testAcquirerId)
                + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION
        );
    }

    public void testGetLocksForAcquirer() {
        String lock1 = new FileLockInfo(testMetadata, testAcquirerId).generateLockName();
        String lock2 = new FileLockInfo(testMetadata, "acquirerId2").generateLockName();

        assertEquals(testAcquirerId, FileLockInfo.getAcquirerIdFromLock(lock1));
        assertEquals("acquirerId2", FileLockInfo.getAcquirerIdFromLock(lock2));
    }
}
