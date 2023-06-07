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
public class RemoteStoreShardShallowCopySnapshot implements ToXContentFragment {

    private final String snapshot;
    private final long indexVersion;
    private final long startTime;
    private final long time;
    private final int incrementalFileCount;
    private final long incrementalSize;
    private final long primaryTerm;
    private final long commitGeneration;
    private final String remoteStoreRepository;

    private final String indexUUID;

    private static final String NAME = "name";
    private static final String INDEX_VERSION = "index_version";
    private static final String START_TIME = "start_time";
    private static final String TIME = "time";

    private static final String INDEX_UUID = "index_uuid";

    private static final String REMOTE_STORE_REPOSITORY = "remote_store_repository";

    private static final String PRIMARY_TERM = "primary_term";

    private static final String COMMIT_GENERATION = "commit_generation";

    private static final String INCREMENTAL_FILE_COUNT = "number_of_files";
    private static final String INCREMENTAL_SIZE = "total_size";

    private static final ParseField PARSE_NAME = new ParseField(NAME);

    private static final ParseField PARSE_PRIMARY_TERM = new ParseField(PRIMARY_TERM);
    private static final ParseField PARSE_COMMIT_GENERATION = new ParseField(COMMIT_GENERATION);
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
        builder.field(INCREMENTAL_FILE_COUNT, incrementalFileCount);
        builder.field(INCREMENTAL_SIZE, incrementalSize);
        builder.field(INDEX_UUID, indexUUID);
        builder.field(REMOTE_STORE_REPOSITORY, remoteStoreRepository);
        builder.field(COMMIT_GENERATION, commitGeneration);
        builder.field(PRIMARY_TERM, primaryTerm);

        return builder;
    }

    public RemoteStoreShardShallowCopySnapshot(
        String snapshot,
        long indexVersion,
        long primaryTerm,
        long commitGeneration,
        long startTime,
        long time,
        int incrementalFileCount,
        long incrementalSize,
        String indexUUID,
        String remoteStoreRepository
    ) {
        assert snapshot != null;
        assert indexVersion >= 0;
        assert commitGeneration >= 0 && primaryTerm >= 0;
        this.snapshot = snapshot;
        this.indexVersion = indexVersion;
        this.primaryTerm = primaryTerm;
        this.commitGeneration = commitGeneration;
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
    public static RemoteStoreShardShallowCopySnapshot fromXContent(XContentParser parser) throws IOException {
        String snapshot = null;
        long indexVersion = -1;
        long startTime = 0;
        long time = 0;
        int incrementalFileCount = 0;
        long incrementalSize = 0;
        String indexUUID = null;
        String remoteStoreRepository = null;
        long primaryTerm = -1;
        long commitGeneration = -1;

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
                        indexVersion = parser.longValue();
                    } else if (PARSE_PRIMARY_TERM.match(currentFieldName, parser.getDeprecationHandler())) {
                        primaryTerm = parser.longValue();
                    } else if (PARSE_COMMIT_GENERATION.match(currentFieldName, parser.getDeprecationHandler())) {
                        commitGeneration = parser.longValue();
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

        return new RemoteStoreShardShallowCopySnapshot(
            snapshot,
            indexVersion,
            primaryTerm,
            commitGeneration,
            startTime,
            time,
            incrementalFileCount,
            incrementalSize,
            indexUUID,
            remoteStoreRepository
        );
    }

    /**
     * Returns shard primary Term during snapshot creation
     *
     * @return primary Term
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns snapshot commit generation
     *
     * @return commit Generation
     */
    public long getCommitGeneration() {
        return commitGeneration;
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
