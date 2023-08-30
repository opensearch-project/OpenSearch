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
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest file which contains the details of the uploaded entity metadata
 *
 * @opensearch.internal
 */
public class ClusterMetadataManifest implements Writeable, ToXContentFragment {

    private static final ParseField CLUSTER_TERM_FIELD = new ParseField("cluster_term");
    private static final ParseField STATE_VERSION_FIELD = new ParseField("state_version");
    private static final ParseField CLUSTER_UUID_FIELD = new ParseField("cluster_uuid");
    private static final ParseField STATE_UUID_FIELD = new ParseField("state_uuid");
    private static final ParseField OPENSEARCH_VERSION_FIELD = new ParseField("opensearch_version");
    private static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    private static final ParseField COMMITTED_FIELD = new ParseField("committed");
    private static final ParseField INDICES_FIELD = new ParseField("indices");

    private static long term(Object[] fields) {
        return (long) fields[0];
    }

    private static long version(Object[] fields) {
        return (long) fields[1];
    }

    private static String clusterUUID(Object[] fields) {
        return (String) fields[2];
    }

    private static String stateUUID(Object[] fields) {
        return (String) fields[3];
    }

    private static Version opensearchVersion(Object[] fields) {
        return Version.fromId((int) fields[4]);
    }

    private static String nodeId(Object[] fields) {
        return (String) fields[5];
    }

    private static boolean committed(Object[] fields) {
        return (boolean) fields[6];
    }

    private static List<UploadedIndexMetadata> indices(Object[] fields) {
        return (List<UploadedIndexMetadata>) fields[7];
    }

    private static final ConstructingObjectParser<ClusterMetadataManifest, Void> PARSER = new ConstructingObjectParser<>(
        "cluster_metadata_manifest",
        fields -> new ClusterMetadataManifest(
            term(fields),
            version(fields),
            clusterUUID(fields),
            stateUUID(fields),
            opensearchVersion(fields),
            nodeId(fields),
            committed(fields),
            indices(fields)
        )
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), CLUSTER_TERM_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), STATE_VERSION_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_UUID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), STATE_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), OPENSEARCH_VERSION_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), COMMITTED_FIELD);
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> UploadedIndexMetadata.fromXContent(p),
            INDICES_FIELD
        );
    }

    private final List<UploadedIndexMetadata> indices;
    private final long clusterTerm;
    private final long stateVersion;
    private final String clusterUUID;
    private final String stateUUID;
    private final Version opensearchVersion;
    private final String nodeId;
    private final boolean committed;

    public List<UploadedIndexMetadata> getIndices() {
        return indices;
    }

    public long getClusterTerm() {
        return clusterTerm;
    }

    public long getStateVersion() {
        return stateVersion;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public Version getOpensearchVersion() {
        return opensearchVersion;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isCommitted() {
        return committed;
    }

    public ClusterMetadataManifest(
        long clusterTerm,
        long version,
        String clusterUUID,
        String stateUUID,
        Version opensearchVersion,
        String nodeId,
        boolean committed,
        List<UploadedIndexMetadata> indices
    ) {
        this.clusterTerm = clusterTerm;
        this.stateVersion = version;
        this.clusterUUID = clusterUUID;
        this.stateUUID = stateUUID;
        this.opensearchVersion = opensearchVersion;
        this.nodeId = nodeId;
        this.committed = committed;
        this.indices = Collections.unmodifiableList(indices);
    }

    public ClusterMetadataManifest(StreamInput in) throws IOException {
        this.clusterTerm = in.readVLong();
        this.stateVersion = in.readVLong();
        this.clusterUUID = in.readString();
        this.stateUUID = in.readString();
        this.opensearchVersion = Version.fromId(in.readInt());
        this.nodeId = in.readString();
        this.committed = in.readBoolean();
        this.indices = Collections.unmodifiableList(in.readList(UploadedIndexMetadata::new));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ClusterMetadataManifest manifest) {
        return new Builder(manifest);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CLUSTER_TERM_FIELD.getPreferredName(), getClusterTerm())
            .field(STATE_VERSION_FIELD.getPreferredName(), getStateVersion())
            .field(CLUSTER_UUID_FIELD.getPreferredName(), getClusterUUID())
            .field(STATE_UUID_FIELD.getPreferredName(), getStateUUID())
            .field(OPENSEARCH_VERSION_FIELD.getPreferredName(), getOpensearchVersion().id)
            .field(NODE_ID_FIELD.getPreferredName(), getNodeId())
            .field(COMMITTED_FIELD.getPreferredName(), isCommitted());
        builder.startArray(INDICES_FIELD.getPreferredName());
        {
            for (UploadedIndexMetadata uploadedIndexMetadata : indices) {
                uploadedIndexMetadata.toXContent(builder, params);
            }
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(clusterTerm);
        out.writeVLong(stateVersion);
        out.writeString(clusterUUID);
        out.writeString(stateUUID);
        out.writeInt(opensearchVersion.id);
        out.writeString(nodeId);
        out.writeBoolean(committed);
        out.writeCollection(indices);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterMetadataManifest that = (ClusterMetadataManifest) o;
        return Objects.equals(indices, that.indices)
            && clusterTerm == that.clusterTerm
            && stateVersion == that.stateVersion
            && Objects.equals(clusterUUID, that.clusterUUID)
            && Objects.equals(stateUUID, that.stateUUID)
            && Objects.equals(opensearchVersion, that.opensearchVersion)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(committed, that.committed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, clusterTerm, stateVersion, clusterUUID, stateUUID, opensearchVersion, nodeId, committed);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    public static ClusterMetadataManifest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Builder for ClusterMetadataManifest
     *
     * @opensearch.internal
     */
    public static class Builder {

        private List<UploadedIndexMetadata> indices;
        private long clusterTerm;
        private long stateVersion;
        private String clusterUUID;
        private String stateUUID;
        private Version opensearchVersion;
        private String nodeId;
        private boolean committed;

        public Builder indices(List<UploadedIndexMetadata> indices) {
            this.indices = indices;
            return this;
        }

        public Builder clusterTerm(long clusterTerm) {
            this.clusterTerm = clusterTerm;
            return this;
        }

        public Builder stateVersion(long stateVersion) {
            this.stateVersion = stateVersion;
            return this;
        }

        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder stateUUID(String stateUUID) {
            this.stateUUID = stateUUID;
            return this;
        }

        public Builder opensearchVersion(Version opensearchVersion) {
            this.opensearchVersion = opensearchVersion;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder committed(boolean committed) {
            this.committed = committed;
            return this;
        }

        public List<UploadedIndexMetadata> getIndices() {
            return indices;
        }

        public Builder() {
            indices = new ArrayList<>();
        }

        public Builder(ClusterMetadataManifest manifest) {
            this.clusterTerm = manifest.clusterTerm;
            this.stateVersion = manifest.stateVersion;
            this.clusterUUID = manifest.clusterUUID;
            this.stateUUID = manifest.stateUUID;
            this.opensearchVersion = manifest.opensearchVersion;
            this.nodeId = manifest.nodeId;
            this.committed = manifest.committed;
            this.indices = new ArrayList<>(manifest.indices);
        }

        public ClusterMetadataManifest build() {
            return new ClusterMetadataManifest(
                clusterTerm,
                stateVersion,
                clusterUUID,
                stateUUID,
                opensearchVersion,
                nodeId,
                committed,
                indices
            );
        }

    }

    /**
     * Metadata for uploaded index metadata
     *
     * @opensearch.internal
     */
    public static class UploadedIndexMetadata implements Writeable, ToXContentFragment {

        private static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
        private static final ParseField INDEX_UUID_FIELD = new ParseField("index_uuid");
        private static final ParseField UPLOADED_FILENAME_FIELD = new ParseField("uploaded_filename");

        private static String indexName(Object[] fields) {
            return (String) fields[0];
        }

        private static String indexUUID(Object[] fields) {
            return (String) fields[1];
        }

        private static String uploadedFilename(Object[] fields) {
            return (String) fields[2];
        }

        private static final ConstructingObjectParser<UploadedIndexMetadata, Void> PARSER = new ConstructingObjectParser<>(
            "uploaded_index_metadata",
            fields -> new UploadedIndexMetadata(indexName(fields), indexUUID(fields), uploadedFilename(fields))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_UUID_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), UPLOADED_FILENAME_FIELD);
        }

        private final String indexName;
        private final String indexUUID;
        private final String uploadedFilename;

        public UploadedIndexMetadata(String indexName, String indexUUID, String uploadedFileName) {
            this.indexName = indexName;
            this.indexUUID = indexUUID;
            this.uploadedFilename = uploadedFileName;
        }

        public UploadedIndexMetadata(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.indexUUID = in.readString();
            this.uploadedFilename = in.readString();
        }

        public String getUploadedFilename() {
            return uploadedFilename;
        }

        public String getIndexName() {
            return indexName;
        }

        public String getIndexUUID() {
            return indexUUID;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(INDEX_NAME_FIELD.getPreferredName(), getIndexName())
                .field(INDEX_UUID_FIELD.getPreferredName(), getIndexUUID())
                .field(UPLOADED_FILENAME_FIELD.getPreferredName(), getUploadedFilename())
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeString(indexUUID);
            out.writeString(uploadedFilename);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final UploadedIndexMetadata that = (UploadedIndexMetadata) o;
            return Objects.equals(indexName, that.indexName)
                && Objects.equals(indexUUID, that.indexUUID)
                && Objects.equals(uploadedFilename, that.uploadedFilename);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, indexUUID, uploadedFilename);
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }

        public static UploadedIndexMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
