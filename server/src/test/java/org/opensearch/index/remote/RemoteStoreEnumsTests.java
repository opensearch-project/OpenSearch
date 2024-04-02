/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStorePathStrategy.PathInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.FIXED;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.parseString;

public class RemoteStoreEnumsTests extends OpenSearchTestCase {

    private static final String SEPARATOR = "/";

    public void testParseString() {
        // Case 1 - Pass values from the enum.
        String typeString = FIXED.toString();
        PathType type = parseString(randomFrom(typeString, typeString.toLowerCase(Locale.ROOT)));
        assertEquals(FIXED, type);

        typeString = PathType.HASHED_PREFIX.toString();
        type = parseString(randomFrom(typeString, typeString.toLowerCase(Locale.ROOT)));
        assertEquals(PathType.HASHED_PREFIX, type);

        // Case 2 - Pass random string
        String randomTypeString = randomAlphaOfLength(2);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> parseString(randomTypeString));
        assertEquals("Could not parse PathType for [" + randomTypeString + "]", ex.getMessage());

        // Case 3 - Null string
        ex = assertThrows(IllegalArgumentException.class, () -> parseString(null));
        assertEquals("Could not parse PathType for [null]", ex.getMessage());
    }

    public void testGeneratePathForFixedType() {
        BlobPath blobPath = new BlobPath();
        List<String> pathList = getPathList();
        for (String path : pathList) {
            blobPath = blobPath.add(path);
        }

        String indexUUID = randomAlphaOfLength(10);
        String shardId = String.valueOf(randomInt(100));
        DataCategory dataCategory = TRANSLOG;
        DataType dataType = DATA;

        String basePath = getPath(pathList) + indexUUID + SEPARATOR + shardId + SEPARATOR;
        // Translog Data
        PathInput pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        BlobPath result = FIXED.path(pathInput, null);
        assertEquals(basePath + dataCategory.getName() + SEPARATOR + dataType.getName() + SEPARATOR, result.buildAsString());

        // Translog Metadata
        dataType = METADATA;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = FIXED.path(pathInput, null);
        assertEquals(basePath + dataCategory.getName() + SEPARATOR + dataType.getName() + SEPARATOR, result.buildAsString());

        // Translog Lock files - This is a negative case where the assertion will trip.
        dataType = LOCK_FILES;
        PathInput finalPathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        assertThrows(AssertionError.class, () -> FIXED.path(finalPathInput, null));

        // Segment Data
        dataCategory = SEGMENTS;
        dataType = DATA;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = FIXED.path(pathInput, null);
        assertEquals(basePath + dataCategory.getName() + SEPARATOR + dataType.getName() + SEPARATOR, result.buildAsString());

        // Segment Metadata
        dataType = METADATA;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = FIXED.path(pathInput, null);
        assertEquals(basePath + dataCategory.getName() + SEPARATOR + dataType.getName() + SEPARATOR, result.buildAsString());

        // Segment Metadata
        dataType = LOCK_FILES;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = FIXED.path(pathInput, null);
        assertEquals(basePath + dataCategory.getName() + SEPARATOR + dataType.getName() + SEPARATOR, result.buildAsString());
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
