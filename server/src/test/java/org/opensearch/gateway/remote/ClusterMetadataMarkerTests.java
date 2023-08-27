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
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.remote.ClusterMetadataMarker.UploadedIndexMetadata;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClusterMetadataMarkerTests extends OpenSearchTestCase {

    public void testClusterMetadataMarkerXContent() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        ClusterMetadataMarker originalMarker = new ClusterMetadataMarker(
            1L,
            1L,
            "test-cluster-uuid",
            "test-state-uuid",
            Version.CURRENT,
            "test-node-id",
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

    public void testClusterMetadataMarkerSerializationEqualsHashCode() {
        ClusterMetadataMarker initialMarker = new ClusterMetadataMarker(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            VersionUtils.randomOpenSearchVersion(random()),
            randomAlphaOfLength(10),
            randomBoolean(),
            randomUploadedIndexMetadataList()
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            initialMarker,
            orig -> OpenSearchTestCase.copyWriteable(orig, new NamedWriteableRegistry(Collections.emptyList()), ClusterMetadataMarker::new),
            marker -> {
                ClusterMetadataMarker.Builder builder = ClusterMetadataMarker.builder(marker);
                switch (randomInt(7)) {
                    case 0:
                        builder.clusterTerm(randomNonNegativeLong());
                        break;
                    case 1:
                        builder.stateVersion(randomNonNegativeLong());
                        break;
                    case 2:
                        builder.clusterUUID(randomAlphaOfLength(10));
                        break;
                    case 3:
                        builder.stateUUID(randomAlphaOfLength(10));
                        break;
                    case 4:
                        builder.opensearchVersion(VersionUtils.randomOpenSearchVersion(random()));
                        break;
                    case 5:
                        builder.nodeId(randomAlphaOfLength(10));
                        break;
                    case 6:
                        builder.committed(randomBoolean());
                        break;
                    case 7:
                        builder.indices(randomUploadedIndexMetadataList());
                        break;
                }
                return builder.build();
            }
        );
    }

    private List<UploadedIndexMetadata> randomUploadedIndexMetadataList() {
        final int size = randomIntBetween(1, 10);
        final List<UploadedIndexMetadata> uploadedIndexMetadataList = new ArrayList<>(size);
        while (uploadedIndexMetadataList.size() < size) {
            assertTrue(uploadedIndexMetadataList.add(randomUploadedIndexMetadata()));
        }
        return uploadedIndexMetadataList;
    }

    private UploadedIndexMetadata randomUploadedIndexMetadata() {
        return new UploadedIndexMetadata(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public void testUploadedIndexMetadataSerializationEqualsHashCode() {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            uploadedIndexMetadata,
            orig -> OpenSearchTestCase.copyWriteable(orig, new NamedWriteableRegistry(Collections.emptyList()), UploadedIndexMetadata::new),
            metadata -> randomlyChangingUploadedIndexMetadata(uploadedIndexMetadata)
        );
    }

    private UploadedIndexMetadata randomlyChangingUploadedIndexMetadata(UploadedIndexMetadata uploadedIndexMetadata) {
        switch (randomInt(2)) {
            case 0:
                return new UploadedIndexMetadata(
                    randomAlphaOfLength(10),
                    uploadedIndexMetadata.getIndexUUID(),
                    uploadedIndexMetadata.getUploadedFilename()
                );
            case 1:
                return new UploadedIndexMetadata(
                    uploadedIndexMetadata.getIndexName(),
                    randomAlphaOfLength(10),
                    uploadedIndexMetadata.getUploadedFilename()
                );
            case 2:
                return new UploadedIndexMetadata(
                    uploadedIndexMetadata.getIndexName(),
                    uploadedIndexMetadata.getIndexUUID(),
                    randomAlphaOfLength(10)
                );
        }
        return uploadedIndexMetadata;
    }
}
