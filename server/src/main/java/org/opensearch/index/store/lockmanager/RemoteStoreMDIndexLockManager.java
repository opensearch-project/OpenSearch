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
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * A Lock Manager Class for Index Level Lock
 * @opensearch.internal
 */
public class RemoteStoreMDIndexLockManager extends RemoteStoreMDLockManager {

    public static final int NO_TTL = -1;
    public static final String LOCK_EXPIRY_TIME = "Lock Expiry Time";
    private static final Logger logger = LogManager.getLogger(RemoteStoreMDIndexLockManager.class);

    public RemoteStoreMDIndexLockManager(RemoteDirectory lockDirectory) {
        super(lockDirectory);
    }

    public void acquireLock(String resourceID, int ttl) throws IOException {
        String lockFile = generateIndexLockFileName(resourceID);
        Map<String, String> lockFileData = getDataForLockFile(resourceID, ttl);
        super.acquireLock(lockFile, lockFileData);
    }

    public Boolean isLockExpired(String lockFile) throws IOException {
        Map<String, String> lockFileData = super.readLockFileContent(lockFile);
        if (lockFileData.containsKey(LOCK_EXPIRY_TIME)) {
            Instant currentInstantInUTC = Instant.now().atZone(ZoneOffset.UTC).toInstant();
            return Instant.parse(lockFileData.get(LOCK_EXPIRY_TIME)).compareTo(currentInstantInUTC) > 0;
        }
        return false;
    }

    private Map<String, String> getDataForLockFile(String resourceID, int ttl) {
        Map<String, String> lockFileData = new HashMap<>();
        lockFileData.put(RESOURCE_ID, resourceID);
        if (ttl != NO_TTL) {
            String expiryDate = Instant.now().atZone(ZoneOffset.UTC).plusDays(ttl).toInstant().toString();
            lockFileData.put(LOCK_EXPIRY_TIME, expiryDate);
        }
        return lockFileData;
    }

    private String getResourceIDFromLockFile(String lockFile) {
        return lockFile.replace(LOCK_FILE_EXTENSION, "");
    }

    public void releaseLock(String resourceID) throws IOException {
        String lockFile = generateIndexLockFileName(resourceID);
        super.releaseLock(lockFile);
    }

    private String generateIndexLockFileName(String resourceID) {
        return resourceID + LOCK_FILE_EXTENSION;
    }
}
