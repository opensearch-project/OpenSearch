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
import java.util.*;

/**
 * A Lock Manager Class for Shard Level Lock {@code RemoteLockDirectory}.
 * @opensearch.internal
 */
public class RemoteStoreMDShardLockManager extends RemoteStoreMDLockManager {
    private static final String METADATA_FILE_NAME = "MD File Name";

    private static final Logger logger = LogManager.getLogger(RemoteStoreMDShardLockManager.class);

    public RemoteStoreMDShardLockManager(RemoteDirectory lockDirectory) {
        super(lockDirectory);
    }

    // lock md file for shard
    public void acquireLock(String metadataFile, String resourceID) throws IOException {
        String lockFile = generateLockFileName(metadataFile, resourceID);
        Map<String, String> lockFileData = getDataForLockFile(metadataFile, resourceID);
        super.acquireLock(lockFile, lockFileData);
    }

    private Map<String, String> getDataForLockFile(String metadataFile, String resourceID) {
        Map<String, String> lockFileData = new HashMap<>();
        lockFileData.put(RESOURCE_ID, resourceID);
        lockFileData.put(METADATA_FILE_NAME, metadataFile);
        return lockFileData;
    }

    public void releaseLock(String metadataFile, String resourceID) throws IOException {
        String lockFile = generateLockFileName(metadataFile, resourceID);
        super.releaseLock(lockFile);
    }

    public String getResourceIDfromLockFile(String lockFile) {
        return lockFile.split(SEPARATOR)[1].replace(LOCK_FILE_EXTENSION, "");
    }

    public String getMetadataFileNameFromLockFile(String lockFile) {
        return lockFile.split(SEPARATOR)[0];
    }

    private String generateLockFileName(String mdFile, String resourceID) {
        return String.join(SEPARATOR, mdFile, resourceID) + LOCK_FILE_EXTENSION;
    }
}
