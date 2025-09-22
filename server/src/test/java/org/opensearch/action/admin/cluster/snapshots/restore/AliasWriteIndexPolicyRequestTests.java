/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.restore;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class AliasWriteIndexPolicyRequestTests extends OpenSearchTestCase {

    public void testEqualsAndHashCode() {
        RestoreSnapshotRequest request1 = new RestoreSnapshotRequest("repo", "snapshot").indices("index1", "index2")
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX)
            .aliasSuffix("_backup");

        RestoreSnapshotRequest request2 = new RestoreSnapshotRequest("repo", "snapshot").indices("index1", "index2")
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX)
            .aliasSuffix("_backup");

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());

        // Test with different policy
        RestoreSnapshotRequest request3 = new RestoreSnapshotRequest("repo", "snapshot").indices("index1", "index2")
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.CUSTOM_SUFFIX)
            .aliasSuffix("_backup");

        assertNotEquals(request1, request3);

        // Test with different suffix
        RestoreSnapshotRequest request4 = new RestoreSnapshotRequest("repo", "snapshot").indices("index1", "index2")
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX)
            .aliasSuffix("_different");

        assertNotEquals(request1, request4);
    }

    public void testSerialization() throws IOException {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest("test-repo", "test-snapshot").indices("index1", "index2")
            .renamePattern("(.+)")
            .renameReplacement("restored-$1")
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.CUSTOM_SUFFIX)
            .aliasSuffix("_restored")
            .includeAliases(true)
            .partial(true)
            .waitForCompletion(false);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RestoreSnapshotRequest deserialized = new RestoreSnapshotRequest(in);

        assertEquals(request.repository(), deserialized.repository());
        assertEquals(request.snapshot(), deserialized.snapshot());
        assertArrayEquals(request.indices(), deserialized.indices());
        assertEquals(request.aliasWriteIndexPolicy(), deserialized.aliasWriteIndexPolicy());
        assertEquals(request.aliasSuffix(), deserialized.aliasSuffix());
        assertEquals(request.includeAliases(), deserialized.includeAliases());
    }

    public void testXContentRoundTrip() throws IOException {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest("test-repo", "test-snapshot").indices("index1", "index2")
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX)
            .aliasSuffix("_test")
            .includeAliases(true)
            .includeGlobalState(false)
            .partial(true);

        // Convert to XContent
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Parse from XContent
        XContentParser parser = createParser(builder);
        Map<String, Object> source = parser.map();
        RestoreSnapshotRequest parsed = new RestoreSnapshotRequest().source(source);

        // Verify key fields
        assertArrayEquals(request.indices(), parsed.indices());
        assertEquals(request.aliasWriteIndexPolicy(), parsed.aliasWriteIndexPolicy());
        assertEquals(request.aliasSuffix(), parsed.aliasSuffix());
        assertEquals(request.includeAliases(), parsed.includeAliases());
        assertEquals(request.includeGlobalState(), parsed.includeGlobalState());
        assertEquals(request.partial(), parsed.partial());
    }

    public void testPolicyFromString() {
        assertEquals(
            RestoreSnapshotRequest.AliasWriteIndexPolicy.PRESERVE,
            RestoreSnapshotRequest.AliasWriteIndexPolicy.fromString("preserve")
        );
        assertEquals(
            RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX,
            RestoreSnapshotRequest.AliasWriteIndexPolicy.fromString("strip_write_index")
        );
        assertEquals(
            RestoreSnapshotRequest.AliasWriteIndexPolicy.CUSTOM_SUFFIX,
            RestoreSnapshotRequest.AliasWriteIndexPolicy.fromString("CUSTOM_SUFFIX")
        );

        expectThrows(IllegalArgumentException.class, () -> RestoreSnapshotRequest.AliasWriteIndexPolicy.fromString("invalid"));
    }

    public void testDefaultValues() {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest();
        assertThat(request.aliasWriteIndexPolicy(), equalTo(RestoreSnapshotRequest.AliasWriteIndexPolicy.PRESERVE));
        assertNull(request.aliasSuffix());
    }
}
