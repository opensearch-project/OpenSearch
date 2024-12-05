/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class SnapshotRequestsTests extends OpenSearchTestCase {
    public void testRestoreSnapshotRequestParsing() throws IOException {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest("test-repo", "test-snap");

        XContentBuilder builder = jsonBuilder().startObject();

        if (randomBoolean()) {
            builder.field("indices", "foo,bar,baz");
        } else {
            builder.startArray("indices");
            builder.value("foo");
            builder.value("bar");
            builder.value("baz");
            builder.endArray();
        }

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        if (indicesOptions.expandWildcardsClosed()) {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "all");
            } else {
                builder.field("expand_wildcards", "closed");
            }
        } else {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "open");
            } else {
                builder.field("expand_wildcards", "none");
            }
        }
        builder.field("allow_no_indices", indicesOptions.allowNoIndices());
        builder.field("rename_pattern", "rename-from");
        builder.field("rename_replacement", "rename-to");
        builder.field("rename_alias_pattern", "alias-rename-from");
        builder.field("rename_alias_replacement", "alias-rename-to");
        boolean partial = randomBoolean();
        builder.field("partial", partial);
        builder.startObject("settings").field("set1", "val1").endObject();
        builder.startObject("index_settings").field("set1", "val2").endObject();
        if (randomBoolean()) {
            builder.field("ignore_index_settings", "set2,set3");
        } else {
            builder.startArray("ignore_index_settings");
            builder.value("set2");
            builder.value("set3");
            builder.endArray();
        }
        boolean includeIgnoreUnavailable = randomBoolean();
        if (includeIgnoreUnavailable) {
            builder.field("ignore_unavailable", indicesOptions.ignoreUnavailable());
        }

        BytesReference bytes = BytesReference.bytes(builder.endObject());

        request.source(XContentHelper.convertToMap(bytes, true, builder.contentType()).v2());

        assertEquals("test-repo", request.repository());
        assertEquals("test-snap", request.snapshot());
        assertArrayEquals(request.indices(), new String[] { "foo", "bar", "baz" });
        assertEquals("rename-from", request.renamePattern());
        assertEquals("rename-to", request.renameReplacement());
        assertEquals("alias-rename-from", request.renameAliasPattern());
        assertEquals("alias-rename-to", request.renameAliasReplacement());
        assertEquals(partial, request.partial());
        assertArrayEquals(request.ignoreIndexSettings(), new String[] { "set2", "set3" });
        boolean expectedIgnoreAvailable = includeIgnoreUnavailable
            ? indicesOptions.ignoreUnavailable()
            : IndicesOptions.strictExpandOpen().ignoreUnavailable();
        assertEquals(expectedIgnoreAvailable, request.indicesOptions().ignoreUnavailable());

        assertWarnings("specifying [settings] when restoring a snapshot has no effect and will not be supported in a future version");
    }

    public void testCreateSnapshotRequestParsing() throws IOException {
        CreateSnapshotRequest request = new CreateSnapshotRequest("test-repo", "test-snap");

        XContentBuilder builder = jsonBuilder().startObject();

        if (randomBoolean()) {
            builder.field("indices", "foo,bar,baz");
        } else {
            builder.startArray("indices");
            builder.value("foo");
            builder.value("bar");
            builder.value("baz");
            builder.endArray();
        }

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        if (indicesOptions.expandWildcardsClosed()) {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "all");
            } else {
                builder.field("expand_wildcards", "closed");
            }
        } else {
            if (indicesOptions.expandWildcardsOpen()) {
                builder.field("expand_wildcards", "open");
            } else {
                builder.field("expand_wildcards", "none");
            }
        }
        builder.field("allow_no_indices", indicesOptions.allowNoIndices());
        boolean partial = randomBoolean();
        builder.field("partial", partial);
        builder.startObject("settings").field("set1", "val1").endObject();
        builder.startObject("index_settings").field("set1", "val2").endObject();
        if (randomBoolean()) {
            builder.field("ignore_index_settings", "set2,set3");
        } else {
            builder.startArray("ignore_index_settings");
            builder.value("set2");
            builder.value("set3");
            builder.endArray();
        }
        boolean includeIgnoreUnavailable = randomBoolean();
        if (includeIgnoreUnavailable) {
            builder.field("ignore_unavailable", indicesOptions.ignoreUnavailable());
        }

        BytesReference bytes = BytesReference.bytes(builder.endObject());

        request.source(XContentHelper.convertToMap(bytes, true, builder.contentType()).v2());

        assertEquals("test-repo", request.repository());
        assertEquals("test-snap", request.snapshot());
        assertArrayEquals(request.indices(), new String[] { "foo", "bar", "baz" });
        assertEquals(partial, request.partial());
        assertEquals("val1", request.settings().get("set1"));
        boolean expectedIgnoreAvailable = includeIgnoreUnavailable
            ? indicesOptions.ignoreUnavailable()
            : IndicesOptions.strictExpandOpen().ignoreUnavailable();
        assertEquals(expectedIgnoreAvailable, request.indicesOptions().ignoreUnavailable());
    }

}
