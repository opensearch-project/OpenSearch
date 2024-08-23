/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import com.fasterxml.jackson.core.JsonParseException;

import org.opensearch.OpenSearchParseException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Snapshot Shard path information.
 *
 * @opensearch.internal
 */
public class SnapshotShardPaths implements ToXContent {

    public static final String DIR = "snapshot_shard_paths";

    public static final String DELIMITER = "#";

    public static final String FILE_NAME_FORMAT = "%s";

    private static final String PATHS_FIELD = "paths";

    private final List<String> paths;

    public SnapshotShardPaths(List<String> paths) {
        this.paths = Collections.unmodifiableList(paths);
    }

    public List<String> getPaths() {
        return paths;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(PATHS_FIELD);
        for (String path : paths) {
            builder.value(path);
        }
        builder.endArray();
        return builder;
    }

    public static SnapshotShardPaths fromXContent(XContentParser parser) throws IOException {
        List<String> paths = new ArrayList<>();

        try {
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }

            if (token != XContentParser.Token.START_OBJECT) {
                throw new OpenSearchParseException("Expected a start object");
            }

            token = parser.nextToken();
            if (token == XContentParser.Token.END_OBJECT) {
                throw new OpenSearchParseException("Missing [" + PATHS_FIELD + "] field");
            }

            while (token != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                if (PATHS_FIELD.equals(fieldName)) {
                    if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                        throw new OpenSearchParseException("Expected an array for field [" + PATHS_FIELD + "]");
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        paths.add(parser.text());
                    }
                } else {
                    throw new OpenSearchParseException("Unexpected field [" + fieldName + "]");
                }
                token = parser.nextToken();
            }
        } catch (JsonParseException e) {
            throw new OpenSearchParseException("Failed to parse SnapshotIndexIdPaths", e);
        }
        return new SnapshotShardPaths(paths);
    }
}
