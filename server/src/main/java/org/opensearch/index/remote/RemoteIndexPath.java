/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStorePathStrategy.PathInput;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.index.remote.RemoteStorePathStrategy.isCompatible;

/**
 * Remote index path information.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RemoteIndexPath implements ToXContentFragment {

    public static final Map<DataCategory, List<DataType>> TRANSLOG_PATH = Map.of(TRANSLOG, List.of(DATA, METADATA));
    public static final Map<DataCategory, List<DataType>> SEGMENT_PATH = Map.of(SEGMENTS, List.of(DataType.values()));
    public static final Map<DataCategory, List<DataType>> COMBINED_PATH;

    static {
        Map<DataCategory, List<DataType>> combinedPath = new HashMap<>();
        combinedPath.putAll(TRANSLOG_PATH);
        combinedPath.putAll(SEGMENT_PATH);
        COMBINED_PATH = Collections.unmodifiableMap(combinedPath);
    }
    private static final String DEFAULT_VERSION = "1";
    public static final String DIR = "remote-index-path";
    public static final String FILE_NAME_FORMAT = "remote_path_%s";
    static final String KEY_VERSION = "version";
    static final String KEY_INDEX_UUID = "index_uuid";
    static final String KEY_SHARD_COUNT = "shard_count";
    static final String KEY_PATH_CREATION_MAP = "path_creation_map";
    static final String KEY_PATHS = "paths";
    private final String indexUUID;
    private final int shardCount;
    private final Iterable<String> basePath;
    private final PathType pathType;
    private final PathHashAlgorithm pathHashAlgorithm;

    /**
     * This keeps the map of paths that would be present in the content of the index path file. For eg - It is possible
     * that segment and translog repository can be different. For this use case, we have either segment or translog as the
     * key, and list of data, metadata, and lock_files (only for segment) as the value.
     */
    private final Map<DataCategory, List<DataType>> pathCreationMap;

    public RemoteIndexPath(
        String indexUUID,
        int shardCount,
        Iterable<String> basePath,
        PathType pathType,
        PathHashAlgorithm pathHashAlgorithm,
        Map<DataCategory, List<DataType>> pathCreationMap
    ) {
        if (Objects.isNull(pathCreationMap)
            || Objects.isNull(pathType)
            || isCompatible(pathType, pathHashAlgorithm) == false
            || shardCount < 1
            || Objects.isNull(basePath)
            || pathCreationMap.isEmpty()
            || pathCreationMap.keySet().stream().anyMatch(k -> pathCreationMap.get(k).isEmpty())) {
            ParameterizedMessage parameterizedMessage = new ParameterizedMessage(
                "Invalid input in RemoteIndexPath constructor indexUUID={} shardCount={} basePath={} pathType={}"
                    + " pathHashAlgorithm={} pathCreationMap={}",
                indexUUID,
                shardCount,
                basePath,
                pathType,
                pathHashAlgorithm,
                pathCreationMap
            );
            throw new IllegalArgumentException(parameterizedMessage.getFormattedMessage());
        }
        boolean validMap = pathCreationMap.keySet()
            .stream()
            .allMatch(k -> pathCreationMap.get(k).stream().allMatch(k::isSupportedDataType));
        if (validMap == false) {
            throw new IllegalArgumentException(
                new ParameterizedMessage("pathCreationMap={} is having illegal combination of category and type", pathCreationMap)
                    .getFormattedMessage()
            );
        }
        this.indexUUID = indexUUID;
        this.shardCount = shardCount;
        this.basePath = basePath;
        this.pathType = pathType;
        this.pathHashAlgorithm = pathHashAlgorithm;
        this.pathCreationMap = pathCreationMap;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_VERSION, DEFAULT_VERSION);
        builder.field(KEY_INDEX_UUID, indexUUID);
        builder.field(KEY_SHARD_COUNT, shardCount);
        builder.field(PathType.NAME, pathType.name());
        if (Objects.nonNull(pathHashAlgorithm)) {
            builder.field(PathHashAlgorithm.NAME, pathHashAlgorithm.name());
        }

        Map<String, List<String>> pathMap = new HashMap<>();
        for (Map.Entry<DataCategory, List<DataType>> entry : pathCreationMap.entrySet()) {
            pathMap.put(entry.getKey().getName(), entry.getValue().stream().map(DataType::getName).collect(Collectors.toList()));
        }
        builder.field(KEY_PATH_CREATION_MAP);
        builder.map(pathMap);
        builder.startArray(KEY_PATHS);
        for (Map.Entry<DataCategory, List<DataType>> entry : pathCreationMap.entrySet()) {
            DataCategory dataCategory = entry.getKey();
            for (DataType type : entry.getValue()) {
                for (int shardNo = 0; shardNo < shardCount; shardNo++) {
                    PathInput pathInput = PathInput.builder()
                        .basePath(new BlobPath().add(basePath))
                        .indexUUID(indexUUID)
                        .shardId(Integer.toString(shardNo))
                        .dataCategory(dataCategory)
                        .dataType(type)
                        .build();
                    builder.value(pathType.path(pathInput, pathHashAlgorithm).buildAsString());
                }
            }
        }
        builder.endArray();
        return builder;
    }

    public static RemoteIndexPath fromXContent(XContentParser ignored) {
        throw new UnsupportedOperationException("RemoteIndexPath.fromXContent() is not supported");
    }
}
