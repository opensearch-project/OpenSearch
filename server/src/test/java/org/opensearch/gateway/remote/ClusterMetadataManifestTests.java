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
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClusterMetadataManifestTests extends OpenSearchTestCase {

    public void testClusterMetadataManifestXContentV0() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        ClusterMetadataManifest originalManifest = new ClusterMetadataManifest(
            1L,
            1L,
            "test-cluster-uuid",
            "test-state-uuid",
            Version.CURRENT,
            "test-node-id",
            false,
            ClusterMetadataManifest.CODEC_V0,
            null,
            Collections.singletonList(uploadedIndexMetadata),
            "prev-cluster-uuid",
            true
        );
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataManifest fromXContentManifest = ClusterMetadataManifest.fromXContentV0(parser);
            assertEquals(originalManifest, fromXContentManifest);
        }
    }

    public void testClusterMetadataManifestXContent() throws IOException {
        UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "test-uuid", "/test/upload/path");
        ClusterMetadataManifest originalManifest = new ClusterMetadataManifest(
            1L,
            1L,
            "test-cluster-uuid",
            "test-state-uuid",
            Version.CURRENT,
            "test-node-id",
            false,
            ClusterMetadataManifest.CODEC_V1,
            "test-global-metadata-file",
            Collections.singletonList(uploadedIndexMetadata),
            "prev-cluster-uuid",
            true
        );
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterMetadataManifest fromXContentManifest = ClusterMetadataManifest.fromXContent(parser);
            assertEquals(originalManifest, fromXContentManifest);
        }
    }

    public void testClusterMetadataManifestSerializationEqualsHashCode() {
        ClusterMetadataManifest initialManifest = new ClusterMetadataManifest(
            1337L,
            7L,
            "HrYF3kP5SmSPWtKlWhnNSA",
            "6By9p9G0Rv2MmFYJcPAOgA",
            Version.CURRENT,
            "B10RX1f5RJenMQvYccCgSQ",
            true,
            1,
            "test-global-metadata-file",
            randomUploadedIndexMetadataList(),
            "yfObdx8KSMKKrXf8UyHhM",
            true
        );
        {  // Mutate Cluster Term
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.clusterTerm(1338L);
                    return builder.build();
                }
            );
        }
        {  // Mutate State Version
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.stateVersion(8L);
                    return builder.build();
                }
            );
        }
        {  // Mutate Cluster UUID
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.clusterUUID("efOkMiPbQZCUQQgtFWdbPw");
                    return builder.build();
                }
            );
        }
        {  // Mutate State UUID
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.stateUUID("efOkMiPbQZCUQQgtFWdbPw");
                    return builder.build();
                }
            );
        }
        {  // Mutate OpenSearch Version
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.opensearchVersion(Version.V_EMPTY);
                    return builder.build();
                }
            );
        }
        {  // Mutate Committed State
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.committed(false);
                    return builder.build();
                }
            );
        }
        {  // Mutate Indices
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.indices(randomUploadedIndexMetadataList());
                    return builder.build();
                }
            );
        }
        { // Mutate Previous cluster UUID
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.previousClusterUUID("vZX62DCQEOzGXlxXCrEu");
                    return builder.build();
                }
            );

        }
        { // Mutate cluster uuid committed
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                initialManifest,
                orig -> OpenSearchTestCase.copyWriteable(
                    orig,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    ClusterMetadataManifest::new
                ),
                manifest -> {
                    ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder(manifest);
                    builder.clusterUUIDCommitted(false);
                    return builder.build();
                }
            );
        }
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
