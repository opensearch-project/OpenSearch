/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.Collections;

public class SnapshotIdTests extends OpenSearchTestCase {
    public void testToXContent() throws IOException {
        SnapshotId snapshotId = new SnapshotId("repo", "snapshot");
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        snapshotId.toXContent(builder, null);
        assertEquals("{\n" + "  \"name\" : \"repo\",\n" + "  \"uuid\" : \"snapshot\"\n" + "}", builder.toString());
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    public void testFromXContentWithRandomField() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        SnapshotId snapshotId = new SnapshotId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        boolean humanReadable = randomBoolean();
        final MediaType mediaType = MediaTypeRegistry.JSON;
        BytesReference originalBytes = toShuffledXContent(
            snapshotId,
            mediaType,
            new ToXContent.MapParams(Collections.emptyMap()),
            humanReadable
        );

        if (addRandomFields) {
            String unsupportedField = "unsupported_field";
            BytesReference mutated = BytesReference.bytes(
                XContentTestUtils.insertIntoXContent(
                    mediaType.xContent(),
                    originalBytes,
                    Collections.singletonList(""),
                    () -> unsupportedField,
                    () -> randomAlphaOfLengthBetween(3, 10)
                )
            );
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> SnapshotId.fromXContent(createParser(mediaType.xContent(), mutated))
            );
            assertEquals("unknown field [" + unsupportedField + "]", iae.getMessage());
        } else {
            try (XContentParser parser = createParser(mediaType.xContent(), originalBytes)) {
                SnapshotId parsedSnapshotId = SnapshotId.fromXContent(parser);
                assertEquals(snapshotId, parsedSnapshotId);
            }
        }
    }
}
