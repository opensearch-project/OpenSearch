/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.message.ParameterizedMessage;
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
import static org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm.FNV_1A;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.FIXED;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_INFIX;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
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

    public void testGeneratePathForHashedPrefixType() {
        BlobPath blobPath = new BlobPath();
        List<String> pathList = getPathList();
        for (String path : pathList) {
            blobPath = blobPath.add(path);
        }

        String indexUUID = randomAlphaOfLength(10);
        String shardId = String.valueOf(randomInt(100));
        DataCategory dataCategory = TRANSLOG;
        DataType dataType = DATA;

        String basePath = getPath(pathList) + indexUUID + SEPARATOR + shardId;
        // Translog Data
        PathInput pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        BlobPath result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertTrue(
            result.buildAsString()
                .startsWith(String.join(SEPARATOR, FNV_1A.hash(pathInput), basePath, dataCategory.getName(), dataType.getName()))
        );

        // assert with exact value for known base path
        BlobPath fixedBlobPath = BlobPath.cleanPath().add("xjsdhj").add("ddjsha").add("yudy7sd").add("32hdhua7").add("89jdij");
        String fixedIndexUUID = "k2ijhe877d7yuhx7";
        String fixedShardId = "10";
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertEquals("DgSI70IciXs/xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/k2ijhe877d7yuhx7/10/translog/data/", result.buildAsString());

        // Translog Metadata
        dataType = METADATA;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertTrue(
            result.buildAsString()
                .startsWith(String.join(SEPARATOR, FNV_1A.hash(pathInput), basePath, dataCategory.getName(), dataType.getName()))
        );

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertEquals("oKU5SjILiy4/xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/k2ijhe877d7yuhx7/10/translog/metadata/", result.buildAsString());

        // Translog Lock files - This is a negative case where the assertion will trip.
        dataType = LOCK_FILES;
        PathInput finalPathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        assertThrows(AssertionError.class, () -> HASHED_PREFIX.path(finalPathInput, null));

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        assertThrows(AssertionError.class, () -> HASHED_PREFIX.path(finalPathInput, null));

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
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertTrue(
            result.buildAsString()
                .startsWith(String.join(SEPARATOR, FNV_1A.hash(pathInput), basePath, dataCategory.getName(), dataType.getName()))
        );

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertEquals("AUBRfCIuWdk/xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/k2ijhe877d7yuhx7/10/segments/data/", result.buildAsString());

        // Segment Metadata
        dataType = METADATA;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertTrue(
            result.buildAsString()
                .startsWith(String.join(SEPARATOR, FNV_1A.hash(pathInput), basePath, dataCategory.getName(), dataType.getName()))
        );

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertEquals("erwR-G735Uw/xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/k2ijhe877d7yuhx7/10/segments/metadata/", result.buildAsString());

        // Segment Lockfiles
        dataType = LOCK_FILES;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertTrue(
            result.buildAsString()
                .startsWith(String.join(SEPARATOR, FNV_1A.hash(pathInput), basePath, dataCategory.getName(), dataType.getName()))
        );

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_PREFIX.path(pathInput, FNV_1A);
        assertEquals("KeYDIk0mJXI/xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/k2ijhe877d7yuhx7/10/segments/lock_files/", result.buildAsString());
    }

    public void testGeneratePathForHashedInfixType() {
        BlobPath blobPath = new BlobPath();
        List<String> pathList = getPathList();
        for (String path : pathList) {
            blobPath = blobPath.add(path);
        }

        String indexUUID = randomAlphaOfLength(10);
        String shardId = String.valueOf(randomInt(100));
        DataCategory dataCategory = TRANSLOG;
        DataType dataType = DATA;

        String basePath = getPath(pathList);
        basePath = basePath.length() == 0 ? basePath : basePath.substring(0, basePath.length() - 1);
        // Translog Data
        PathInput pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        BlobPath result = HASHED_INFIX.path(pathInput, FNV_1A);
        String expected = derivePath(basePath, pathInput);
        String actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // assert with exact value for known base path
        BlobPath fixedBlobPath = BlobPath.cleanPath().add("xjsdhj").add("ddjsha").add("yudy7sd").add("32hdhua7").add("89jdij");
        String fixedIndexUUID = "k2ijhe877d7yuhx7";
        String fixedShardId = "10";
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = "xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/DgSI70IciXs/k2ijhe877d7yuhx7/10/translog/data/";
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // Translog Metadata
        dataType = METADATA;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();

        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = derivePath(basePath, pathInput);
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = "xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/oKU5SjILiy4/k2ijhe877d7yuhx7/10/translog/metadata/";
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // Translog Lock files - This is a negative case where the assertion will trip.
        dataType = LOCK_FILES;
        PathInput finalPathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        assertThrows(AssertionError.class, () -> HASHED_INFIX.path(finalPathInput, null));

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        assertThrows(AssertionError.class, () -> HASHED_INFIX.path(finalPathInput, null));

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
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = derivePath(basePath, pathInput);
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = "xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/AUBRfCIuWdk/k2ijhe877d7yuhx7/10/segments/data/";
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // Segment Metadata
        dataType = METADATA;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = derivePath(basePath, pathInput);
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = "xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/erwR-G735Uw/k2ijhe877d7yuhx7/10/segments/metadata/";
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // Segment Lockfiles
        dataType = LOCK_FILES;
        pathInput = PathInput.builder()
            .basePath(blobPath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = derivePath(basePath, pathInput);
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));

        // assert with exact value for known base path
        pathInput = PathInput.builder()
            .basePath(fixedBlobPath)
            .indexUUID(fixedIndexUUID)
            .shardId(fixedShardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .build();
        result = HASHED_INFIX.path(pathInput, FNV_1A);
        expected = "xjsdhj/ddjsha/yudy7sd/32hdhua7/89jdij/KeYDIk0mJXI/k2ijhe877d7yuhx7/10/segments/lock_files/";
        actual = result.buildAsString();
        assertTrue(new ParameterizedMessage("expected={} actual={}", expected, actual).getFormattedMessage(), actual.startsWith(expected));
    }

    private String derivePath(String basePath, PathInput pathInput) {
        return "".equals(basePath)
            ? String.join(
                SEPARATOR,
                FNV_1A.hash(pathInput),
                pathInput.indexUUID(),
                pathInput.shardId(),
                pathInput.dataCategory().getName(),
                pathInput.dataType().getName()
            )
            : String.join(
                SEPARATOR,
                basePath,
                FNV_1A.hash(pathInput),
                pathInput.indexUUID(),
                pathInput.shardId(),
                pathInput.dataCategory().getName(),
                pathInput.dataType().getName()
            );
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
