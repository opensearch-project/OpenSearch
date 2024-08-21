/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Wrapper class for uploading/downloading {@link ClusterMetadataManifest} to/from remote blob store
 */
public class RemoteClusterMetadataManifest extends AbstractClusterMetadataWriteableBlobEntity<ClusterMetadataManifest> {

    public static final String MANIFEST = "manifest";
    public static final int SPLITTED_MANIFEST_FILE_LENGTH = 6;

    public static final String METADATA_MANIFEST_NAME_FORMAT = "%s";
    public static final int MANIFEST_CURRENT_CODEC_VERSION = ClusterMetadataManifest.CODEC_V3;
    public static final String COMMITTED = "C";
    public static final String PUBLISHED = "P";

    /**
     * Manifest format compatible with older codec v0, where codec version was missing.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V0 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV0);
    /**
     * Manifest format compatible with older codec v1, where global metadata was missing.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V1 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV1);

    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V2 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV2);

    /**
     * Manifest format compatible with codec v2, where we introduced codec versions/global metadata.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT = new ChecksumBlobStoreFormat<>(
        "cluster-metadata-manifest",
        METADATA_MANIFEST_NAME_FORMAT,
        ClusterMetadataManifest::fromXContent
    );

    private ClusterMetadataManifest clusterMetadataManifest;

    public RemoteClusterMetadataManifest(
        final ClusterMetadataManifest clusterMetadataManifest,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.clusterMetadataManifest = clusterMetadataManifest;
    }

    public RemoteClusterMetadataManifest(
        final String blobName,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(MANIFEST), MANIFEST);
    }

    @Override
    public String getType() {
        return MANIFEST;
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest/manifest__<inverted_term>__<inverted_version>__C/P__<inverted__timestamp>__
        // <codec_version>
        String blobFileName = String.join(
            DELIMITER,
            MANIFEST,
            RemoteStoreUtils.invertLong(clusterMetadataManifest.getClusterTerm()),
            RemoteStoreUtils.invertLong(clusterMetadataManifest.getStateVersion()),
            (clusterMetadataManifest.isCommitted() ? COMMITTED : PUBLISHED),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(clusterMetadataManifest.getCodecVersion())
            // Keep the codec version at last place only, during we read last place to determine codec version.
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(MANIFEST, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return CLUSTER_METADATA_MANIFEST_FORMAT.serialize(
            clusterMetadataManifest,
            generateBlobFileName(),
            getCompressor(),
            RemoteClusterStateUtils.FORMAT_PARAMS
        ).streamInput();
    }

    @Override
    public ClusterMetadataManifest deserialize(final InputStream inputStream) throws IOException {
        ChecksumBlobStoreFormat<ClusterMetadataManifest> blobStoreFormat = getClusterMetadataManifestBlobStoreFormat();
        return blobStoreFormat.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

    // package private for testing
    int getManifestCodecVersion() {
        assert blobName != null;
        String[] splitName = getBlobFileName().split(DELIMITER);
        if (splitName.length == SPLITTED_MANIFEST_FILE_LENGTH) {
            return Integer.parseInt(splitName[splitName.length - 1]); // Last value would be codec version.
        } else if (splitName.length < SPLITTED_MANIFEST_FILE_LENGTH) { // Where codec is not part of file name, i.e. default codec version 0
            // is used.
            return ClusterMetadataManifest.CODEC_V0;
        } else {
            throw new IllegalArgumentException("Manifest file name is corrupted : " + blobName);
        }
    }

    private ChecksumBlobStoreFormat<ClusterMetadataManifest> getClusterMetadataManifestBlobStoreFormat() {
        long codecVersion = getManifestCodecVersion();
        if (codecVersion == MANIFEST_CURRENT_CODEC_VERSION) {
            return CLUSTER_METADATA_MANIFEST_FORMAT;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V2) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V2;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V1) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V1;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V0) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V0;
        }
        throw new IllegalArgumentException("Cluster metadata manifest file is corrupted, don't have valid codec version");
    }

}
