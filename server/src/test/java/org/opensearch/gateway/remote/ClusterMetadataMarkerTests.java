/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.remote.ClusterMetadataMarker.UploadedIndexMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

public class ClusterMetadataMarkerTests extends OpenSearchTestCase {

    public void testXContent() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        ClusterMetadataMarker originalMarker = new ClusterMetadataMarker(
            1L,
            1L,
            "test-cluster-uuid",
            "test-state-uuid",
            Version.CURRENT,
            false,
            Collections.singletonList(uploadedIndexMetadata)
        );
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalMarker.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataMarker fromXContentMarker = ClusterMetadataMarker.fromXContent(parser);
            assertEquals(originalMarker, fromXContentMarker);
        }
    }
}
