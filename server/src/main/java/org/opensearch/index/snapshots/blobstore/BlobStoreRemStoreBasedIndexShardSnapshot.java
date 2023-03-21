/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.snapshots.blobstore;

import org.opensearch.OpenSearchParseException;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Remote Store based Shard snapshot metadata
 *
 * @opensearch.internal
 */
public class BlobStoreRemStoreBasedIndexShardSnapshot implements ToXContentFragment {

    private final String snapshot;
    private final long indexVersion;
    private final long startTime;
    private final long time;
    private final int incrementalFileCount;
    private final long incrementalSize;
    private final String remoteStoreMetadataFileName;

    private final String remoteStoreRepository;

    private final String indexUUID;

    private static final String NAME = "name";
    private static final String INDEX_VERSION = "index_version";
    private static final String START_TIME = "start_time";
    private static final String TIME = "time";

    private static final String INDEX_UUID = "index_uuid";

    private static final String REMOTE_STORE_REPOSITORY = "remote_store_repository";

    private static final String REMOTE_STORE_MD_FILE_NAME = "remote_store_md_file_name";
    // for the sake of BWC keep the actual property names as in 6.x
    // + there is a constraint in #fromXContent() that leads to OpenSearchParseException("unknown parameter [incremental_file_count]");
    private static final String INCREMENTAL_FILE_COUNT = "number_of_files";
    private static final String INCREMENTAL_SIZE = "total_size";

    private static final ParseField PARSE_NAME = new ParseField(NAME);

    private static final ParseField PARSE_REMOTE_STORE_MD_FILE_NAME = new ParseField(REMOTE_STORE_MD_FILE_NAME);
    private static final ParseField PARSE_INDEX_VERSION = new ParseField(INDEX_VERSION, "index-version");
    private static final ParseField PARSE_START_TIME = new ParseField(START_TIME);
    private static final ParseField PARSE_TIME = new ParseField(TIME);
    private static final ParseField PARSE_INCREMENTAL_FILE_COUNT = new ParseField(INCREMENTAL_FILE_COUNT);
    private static final ParseField PARSE_INCREMENTAL_SIZE = new ParseField(INCREMENTAL_SIZE);

    private static final ParseField PARSE_INDEX_UUID = new ParseField(INDEX_UUID);

    private static final ParseField PARSE_REMOTE_STORE_REPOSITORY = new ParseField(REMOTE_STORE_REPOSITORY);

    /**
     * Serializes shard snapshot metadata info into JSON
     *
     * @param builder  XContent builder
     * @param params   parameters
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, snapshot);
        builder.field(INDEX_VERSION, indexVersion);
        builder.field(START_TIME, startTime);
        builder.field(TIME, time);
        builder.field(REMOTE_STORE_MD_FILE_NAME, remoteStoreMetadataFileName);
        builder.field(INCREMENTAL_FILE_COUNT, incrementalFileCount);
        builder.field(INCREMENTAL_SIZE, incrementalSize);
        builder.field(INDEX_UUID, indexUUID);
        builder.field(REMOTE_STORE_REPOSITORY, remoteStoreRepository);

        return builder;
    }

    public BlobStoreRemStoreBasedIndexShardSnapshot(
        String snapshot,
        long indexVersion,
        String remoteStoreMetadataFileName,
        long startTime,
        long time,
        int incrementalFileCount,
        long incrementalSize,
        String indexUUID,
        String remoteStoreRepository
    ) {
        assert snapshot != null;
        assert indexVersion >= 0;
        this.snapshot = snapshot;
        this.indexVersion = indexVersion;
        this.remoteStoreMetadataFileName = remoteStoreMetadataFileName;
        this.startTime = startTime;
        this.time = time;
        this.incrementalFileCount = incrementalFileCount;
        this.incrementalSize = incrementalSize;
        this.indexUUID = indexUUID;
        this.remoteStoreRepository = remoteStoreRepository;
    }

    /**
     * Parses shard snapshot metadata
     *
     * @param parser parser
     * @return shard snapshot metadata
     */
    public static BlobStoreRemStoreBasedIndexShardSnapshot fromXContent(XContentParser parser) throws IOException {
        String snapshot = null;
        String remoteStoreMetadataFileName = null;
        long indexVersion = -1;
        long startTime = 0;
        long time = 0;
        int incrementalFileCount = 0;
        long incrementalSize = 0;
        String indexUUID = null;
        String remoteStoreRepository = null;

        List<BlobStoreIndexShardSnapshot.FileInfo> indexFiles = new ArrayList<>();
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                final String currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (token.isValue()) {
                    if (PARSE_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                        snapshot = parser.text();
                    } else if (PARSE_INDEX_VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                        // The index-version is needed for backward compatibility with v 1.0
                        indexVersion = parser.longValue();
                    } else if (PARSE_REMOTE_STORE_MD_FILE_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                        remoteStoreMetadataFileName = parser.text();
                    } else if (PARSE_START_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                        startTime = parser.longValue();
                    } else if (PARSE_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                        time = parser.longValue();
                    } else if (PARSE_INCREMENTAL_FILE_COUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                        incrementalFileCount = parser.intValue();
                    } else if (PARSE_INCREMENTAL_SIZE.match(currentFieldName, parser.getDeprecationHandler())) {
                        incrementalSize = parser.longValue();
                    } else if (PARSE_INDEX_UUID.match(currentFieldName, parser.getDeprecationHandler())) {
                        indexUUID = parser.text();
                    } else if (PARSE_REMOTE_STORE_REPOSITORY.match(currentFieldName, parser.getDeprecationHandler())) {
                        remoteStoreRepository = parser.text();
                    } else {
                        throw new OpenSearchParseException("unknown parameter [{}]", currentFieldName);
                    }
                } else {
                    throw new OpenSearchParseException("unexpected token [{}]", token);
                }
            }
        }

        return new BlobStoreRemStoreBasedIndexShardSnapshot(
            snapshot,
            indexVersion,
            remoteStoreMetadataFileName,
            startTime,
            time,
            incrementalFileCount,
            incrementalSize,
            indexUUID,
            remoteStoreRepository
        );
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String getRemoteStoreMetadataFileName() {
        return remoteStoreMetadataFileName;
    }

    /**
     * Returns Index UUID
     *
     * @return index UUID
     */
    public String getIndexUUID() {
        return indexUUID;
    }

    public String getRemoteStoreRepository() {
        return remoteStoreRepository;
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return snapshot;
    }

    /**
     * Returns list of files in the shard
     *
     * @return list of files
     */

    /**
     * Returns snapshot start time
     */
    public long startTime() {
        return startTime;
    }

    /**
     * Returns snapshot running time
     */
    public long time() {
        return time;
    }

    /**
     * Returns incremental of files that were snapshotted
     */
    public int incrementalFileCount() {
        return incrementalFileCount;
    }

    /**
     * Returns total number of files that are referenced by this snapshot
     */
    public int totalFileCount() {
        return 0;
    }

    /**
     * Returns incremental of files size that were snapshotted
     */
    public long incrementalSize() {
        return incrementalSize;
    }

    /**
     * Returns total size of all files that where snapshotted
     */
    public long totalSize() {
        return 0;
    }


}
