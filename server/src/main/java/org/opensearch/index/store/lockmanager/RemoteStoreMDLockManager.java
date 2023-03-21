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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;
import java.util.Map;

/**
 * A Lock Manager Interface for Remote Store md locks {@code RemoteDirectory}.
 * @opensearch.internal
 */
public abstract class RemoteStoreMDLockManager {
    private static final Logger logger = LogManager.getLogger(RemoteStoreMDLockManager.class);
    private final RemoteDirectory lockDirectory;
    protected static final String SEPARATOR = "__";
    protected static final String LOCK_FILE_EXTENSION = ".lock";
    protected  static final String RESOURCE_ID = "Resource ID";

    protected RemoteStoreMDLockManager(RemoteDirectory lockDirectory) {
        this.lockDirectory = lockDirectory;
    }

    public Map<String, String> readLockFileContent(String lockFile) throws IOException {
        try (IndexInput indexInput = lockDirectory.openInput(lockFile, IOContext.DEFAULT)) {
            return indexInput.readMapOfStrings();
        }
    }

    public void acquireLock(String lockFile, Map<String, String> lockFileData) throws IOException {
        try (IndexOutput indexOutput = lockDirectory.createOutput(lockFile, IOContext.DEFAULT)) {
            indexOutput.writeMapOfStrings(lockFileData);
        }
    }

    public void releaseLock(String lockFile) throws IOException {
        lockDirectory.deleteFile(lockFile);
    }
}
