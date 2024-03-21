/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.opensearch.index.remote.RemoteStorePathType.FIXED;
import static org.opensearch.index.remote.RemoteStorePathType.parseString;
import static org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory.SEGMENTS;
import static org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory.LOCK_FILES;
import static org.opensearch.index.translog.RemoteFsTranslog.DATA_DIR;
import static org.opensearch.index.translog.RemoteFsTranslog.METADATA_DIR;
import static org.opensearch.index.translog.RemoteFsTranslog.TRANSLOG;

public class RemoteStorePathTypeTests extends OpenSearchTestCase {

    private static final String SEPARATOR = "/";

    public void testParseString() {
        // Case 1 - Pass values from the enum.
        String typeString = FIXED.toString();
        RemoteStorePathType type = parseString(randomFrom(typeString, typeString.toLowerCase(Locale.ROOT)));
        assertEquals(FIXED, type);

        typeString = RemoteStorePathType.HASHED_PREFIX.toString();
        type = parseString(randomFrom(typeString, typeString.toLowerCase(Locale.ROOT)));
        assertEquals(RemoteStorePathType.HASHED_PREFIX, type);

        // Case 2 - Pass random string
        String randomTypeString = randomAlphaOfLength(2);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> parseString(randomTypeString));
        assertEquals("Could not parse RemoteStorePathType for [" + randomTypeString + "]", ex.getMessage());

        // Case 3 - Null string
        ex = assertThrows(IllegalArgumentException.class, () -> parseString(null));
        assertEquals("Could not parse RemoteStorePathType for [null]", ex.getMessage());
    }

    public void testGeneratePathForFixedType() {
        BlobPath blobPath = new BlobPath();
        List<String> pathList = getPathList();
        for (String path : pathList) {
            blobPath = blobPath.add(path);
        }

        String indexUUID = randomAlphaOfLength(10);
        String shardId = String.valueOf(randomInt(100));
        String dataCategory = TRANSLOG;
        String dataType = DATA_DIR;

        String basePath = getPath(pathList) + indexUUID + SEPARATOR + shardId + SEPARATOR;
        // Translog Data
        BlobPath result = FIXED.path(blobPath, indexUUID, shardId, dataCategory, dataType);
        assertEquals(basePath + dataCategory + SEPARATOR + dataType + SEPARATOR, result.buildAsString());

        // Translog Metadata
        dataType = METADATA_DIR;
        result = FIXED.path(blobPath, indexUUID, shardId, dataCategory, dataType);
        assertEquals(basePath + dataCategory + SEPARATOR + dataType + SEPARATOR, result.buildAsString());

        // Translog Lock files - This is a negative case where the assertion will trip.
        BlobPath finalBlobPath = blobPath;
        assertThrows(AssertionError.class, () -> FIXED.path(finalBlobPath, indexUUID, shardId, TRANSLOG, LOCK_FILES));

        // Segment Data
        dataCategory = SEGMENTS;
        dataType = DATA_DIR;
        result = FIXED.path(blobPath, indexUUID, shardId, dataCategory, dataType);
        assertEquals(basePath + dataCategory + SEPARATOR + dataType + SEPARATOR, result.buildAsString());

        // Segment Metadata
        dataType = METADATA_DIR;
        result = FIXED.path(blobPath, indexUUID, shardId, dataCategory, dataType);
        assertEquals(basePath + dataCategory + SEPARATOR + dataType + SEPARATOR, result.buildAsString());

        // Segment Metadata
        dataType = LOCK_FILES;
        result = FIXED.path(blobPath, indexUUID, shardId, dataCategory, dataType);
        assertEquals(basePath + dataCategory + SEPARATOR + dataType + SEPARATOR, result.buildAsString());
    }

    private List<String> getPathList() {
        List<String> pathList = new ArrayList<>();
        int length = randomIntBetween(0, 5);
        for (int i = 0; i < length; i++) {
            pathList.add(randomAlphaOfLength(randomIntBetween(2, 5)));
        }
        return pathList;
    }

    private String getPath(List<String> pathList) {
        String p = String.join(SEPARATOR, pathList);
        if (p.isEmpty() || p.endsWith(SEPARATOR)) {
            return p;
        }
        return p + SEPARATOR;
    }
}
