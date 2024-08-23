/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SnapshotShardPathsTests extends OpenSearchTestCase {

    public void testToXContent() throws IOException {
        List<String> paths = Arrays.asList("path1", "path2", "path3");
        SnapshotShardPaths indexIdPaths = new SnapshotShardPaths(paths);

        BytesReference bytes = XContentHelper.toXContent(indexIdPaths, XContentType.JSON, false);
        String expectedJson = "{\"paths\":[\"path1\",\"path2\",\"path3\"]}";
        assertEquals(expectedJson, bytes.utf8ToString());
    }

    public void testToXContentEmptyPaths() throws IOException {
        SnapshotShardPaths indexIdPaths = new SnapshotShardPaths(Collections.emptyList());

        BytesReference bytes = XContentHelper.toXContent(indexIdPaths, XContentType.JSON, false);
        String expectedJson = "{\"paths\":[]}";
        assertEquals(expectedJson, bytes.utf8ToString());
    }

    public void testFromXContent() throws IOException {
        String json = "{\"paths\":[\"path1\",\"path2\",\"path3\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            SnapshotShardPaths indexIdPaths = SnapshotShardPaths.fromXContent(parser);
            List<String> expectedPaths = Arrays.asList("path1", "path2", "path3");
            assertEquals(expectedPaths, indexIdPaths.getPaths());
        }
    }

    public void testFromXContentEmptyPaths() throws IOException {
        String json = "{\"paths\":[]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            SnapshotShardPaths indexIdPaths = SnapshotShardPaths.fromXContent(parser);
            assertEquals(Collections.emptyList(), indexIdPaths.getPaths());
        }
    }

    public void testFromXContentMissingPaths() throws IOException {
        String json = "{}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            OpenSearchParseException exception = expectThrows(
                OpenSearchParseException.class,
                () -> SnapshotShardPaths.fromXContent(parser)
            );
            assertEquals("Missing [paths] field", exception.getMessage());
        }
    }

    public void testFromXContentInvalidJson() throws IOException {
        String json = "{\"paths\":\"invalid\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            OpenSearchParseException exception = expectThrows(
                OpenSearchParseException.class,
                () -> SnapshotShardPaths.fromXContent(parser)
            );
            assertEquals("Expected an array for field [paths]", exception.getMessage());
        }
    }

    public void testFromXContentInvalidJsonObject() throws IOException {
        String json = "invalid";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            OpenSearchParseException exception = expectThrows(
                OpenSearchParseException.class,
                () -> SnapshotShardPaths.fromXContent(parser)
            );
            assertEquals("Failed to parse SnapshotIndexIdPaths", exception.getMessage());
        }
    }

    public void testFromXContentNotAnObject() throws IOException {
        String json = "[]";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            OpenSearchParseException exception = expectThrows(
                OpenSearchParseException.class,
                () -> SnapshotShardPaths.fromXContent(parser)
            );
            assertEquals("Expected a start object", exception.getMessage());
        }
    }
}
