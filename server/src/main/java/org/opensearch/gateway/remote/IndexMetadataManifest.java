/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest for index metadata that tracks all index-related changes separately from cluster manifest
 *
 * @opensearch.internal
 */
public class IndexMetadataManifest implements Writeable, ToXContentFragment {

    public static final int CODEC_V1 = 1;
    public static final int MANIFEST_CURRENT_CODEC_VERSION = CODEC_V1;

    private static final ParseField OPENSEARCH_VERSION_FIELD = new ParseField("opensearch_version");
    private static final ParseField CODEC_VERSION_FIELD = new ParseField("codec_version");
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField PREVIOUS_INDEX_MANIFEST_VERSION_FIELD = new ParseField("previous_index_manifest_version");
    private static final ParseField INDEX_DIFF_MANIFEST_FIELD = new ParseField("index_diff_manifest");

    private static final ConstructingObjectParser<IndexMetadataManifest, Void> PARSER = new ConstructingObjectParser<>(
        "index_metadata_manifest",
        fields -> manifestBuilder(fields).build()
    );

    static {
        declareParser(PARSER, MANIFEST_CURRENT_CODEC_VERSION);
    }

    private static void declareParser(ConstructingObjectParser<IndexMetadataManifest, Void> parser, int codecVersion) {
        parser.declareInt(ConstructingObjectParser.constructorArg(), OPENSEARCH_VERSION_FIELD);
        parser.declareInt(ConstructingObjectParser.constructorArg(), CODEC_VERSION_FIELD);
        parser.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> UploadedIndexMetadata.fromXContent(p, codecVersion >= MANIFEST_CURRENT_CODEC_VERSION ? ClusterMetadataManifest.CODEC_V2 : ClusterMetadataManifest.CODEC_V0),
            INDICES_FIELD
        );
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), PREVIOUS_INDEX_MANIFEST_VERSION_FIELD);
        parser.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> IndexStateDiffManifest.fromXContent(p),
            INDEX_DIFF_MANIFEST_FIELD
        );
    }

    private final Version opensearchVersion;
    private final int codecVersion;
    private final List<UploadedIndexMetadata> indices;
    private final String previousIndexManifestVersion;
    private final IndexStateDiffManifest indexDiffManifest;

    public IndexMetadataManifest(
        Version opensearchVersion,
        int codecVersion,
        List<UploadedIndexMetadata> indices,
        String previousIndexManifestVersion,
        IndexStateDiffManifest indexDiffManifest
    ) {
        this.opensearchVersion = opensearchVersion;
        this.codecVersion = codecVersion;
        this.indices = Collections.unmodifiableList(indices != null ? indices : new ArrayList<>());
        this.previousIndexManifestVersion = previousIndexManifestVersion;
        this.indexDiffManifest = indexDiffManifest;
    }

    public IndexMetadataManifest(StreamInput in) throws IOException {
        this.opensearchVersion = Version.fromId(in.readInt());
        this.codecVersion = in.readInt();
        this.indices = Collections.unmodifiableList(in.readList(UploadedIndexMetadata::new));
        this.previousIndexManifestVersion = in.readOptionalString();
        this.indexDiffManifest = in.readOptionalWriteable(IndexStateDiffManifest::new);
    }

    public Version getOpensearchVersion() {
        return opensearchVersion;
    }

    public int getCodecVersion() {
        return codecVersion;
    }

    public List<UploadedIndexMetadata> getIndices() {
        return indices;
    }

    public String getPreviousIndexManifestVersion() {
        return previousIndexManifestVersion;
    }

    public IndexStateDiffManifest getIndexDiffManifest() {
        return indexDiffManifest;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(opensearchVersion.id);
        out.writeInt(codecVersion);
        out.writeCollection(indices);
        out.writeOptionalString(previousIndexManifestVersion);
        out.writeOptionalWriteable(indexDiffManifest);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(OPENSEARCH_VERSION_FIELD.getPreferredName(), getOpensearchVersion().id)
            .field(CODEC_VERSION_FIELD.getPreferredName(), getCodecVersion());

        builder.startArray(INDICES_FIELD.getPreferredName());
        for (UploadedIndexMetadata uploadedIndexMetadata : indices) {
            builder.startObject();
            uploadedIndexMetadata.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();

        if (previousIndexManifestVersion != null) {
            builder.field(PREVIOUS_INDEX_MANIFEST_VERSION_FIELD.getPreferredName(), previousIndexManifestVersion);
        }

        if (indexDiffManifest != null) {
            builder.startObject(INDEX_DIFF_MANIFEST_FIELD.getPreferredName());
            indexDiffManifest.toXContent(builder, params);
            builder.endObject();
        }

        return builder;
    }

    public static IndexMetadataManifest fromXContent(XContentParser parser) throws IOException {
        // Try to peek at codec_version to determine which parser to use
        // For backward compatibility, assume V1 if codec_version is not present
        return PARSER.parse(parser, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(IndexMetadataManifest manifest) {
        return new Builder(manifest);
    }

    private static IndexMetadataManifest.Builder manifestBuilder(Object[] fields) {
        return IndexMetadataManifest.builder()
            .opensearchVersion(opensearchVersion(fields))
            .codecVersion(CODEC_V1)
            .indices(indices(fields))
            .previousIndexManifestVersion(previousIndexManifestVersion(fields))
            .indexDiffManifest(indexDiffManifest(fields));
    }

    private static Version opensearchVersion(Object[] fields) {
        return Version.fromId((int) fields[0]);
    }

    private static List<UploadedIndexMetadata> indices(Object[] fields) {
        return (List<UploadedIndexMetadata>) fields[2];
    }

    private static String previousIndexManifestVersion(Object[] fields) {
        return (String) fields[3];
    }

    private static IndexStateDiffManifest indexDiffManifest(Object[] fields) {
        return (IndexStateDiffManifest) fields[4];
    }

    public static class Builder {
        private Version opensearchVersion;
        private int codecVersion;
        private List<UploadedIndexMetadata> indices;
        private String previousIndexManifestVersion;
        private IndexStateDiffManifest indexDiffManifest;

        public Builder() {
            indices = new ArrayList<>();
        }

        public Builder(IndexMetadataManifest manifest) {
            this.opensearchVersion = manifest.opensearchVersion;
            this.codecVersion = manifest.codecVersion;
            this.indices = new ArrayList<>(manifest.indices);
            this.previousIndexManifestVersion = manifest.previousIndexManifestVersion;
            this.indexDiffManifest = manifest.indexDiffManifest;
        }

        public Builder opensearchVersion(Version opensearchVersion) {
            this.opensearchVersion = opensearchVersion;
            return this;
        }

        public Builder codecVersion(int codecVersion) {
            this.codecVersion = codecVersion;
            return this;
        }

        public Builder indices(List<UploadedIndexMetadata> indices) {
            this.indices = indices;
            return this;
        }

        public Builder previousIndexManifestVersion(String previousIndexManifestVersion) {
            this.previousIndexManifestVersion = previousIndexManifestVersion;
            return this;
        }

        public Builder indexDiffManifest(IndexStateDiffManifest indexDiffManifest) {
            this.indexDiffManifest = indexDiffManifest;
            return this;
        }

        public IndexMetadataManifest build() {
            return new IndexMetadataManifest(
                opensearchVersion,
                codecVersion,
                indices,
                previousIndexManifestVersion,
                indexDiffManifest
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexMetadataManifest that = (IndexMetadataManifest) o;
        return codecVersion == that.codecVersion
            && Objects.equals(opensearchVersion, that.opensearchVersion)
            && Objects.equals(indices, that.indices)
            && Objects.equals(previousIndexManifestVersion, that.previousIndexManifestVersion)
            && Objects.equals(indexDiffManifest, that.indexDiffManifest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            opensearchVersion,
            codecVersion,
            indices,
            previousIndexManifestVersion,
            indexDiffManifest
        );
    }

    @Override
    public String toString() {
        return "IndexMetadataManifest{" +
            "opensearchVersion=" + opensearchVersion +
            ", codecVersion=" + codecVersion +
            ", indices=" + indices.size() +
            ", previousIndexManifestVersion='" + previousIndexManifestVersion + '\'' +
            ", indexDiffManifest=" + indexDiffManifest +
            '}';
    }
}
