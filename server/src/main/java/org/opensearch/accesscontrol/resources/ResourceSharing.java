/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a resource sharing configuration that manages access control for OpenSearch resources.
 * This class holds information about shared resources including their source, creator, and sharing permissions.
 *
 * <p>This class implements {@link ToXContentFragment} for JSON serialization and {@link NamedWriteable}
 * for stream-based serialization.</p>
 *
 * The class maintains information about:
 * <ul>
 *   <li>The source index where the resource is defined</li>
 *   <li>The unique identifier of the resource</li>
 *   <li>The creator's information</li>
 *   <li>The sharing permissions and recipients</li>
 * </ul>
 *
 *
 * @see CreatedBy
 * @see ShareWith
 * @opensearch.experimental
 */
public class ResourceSharing implements ToXContentFragment, NamedWriteable {

    /**
     * The index where the resource is defined
     */
    private String sourceIdx;

    /**
     * The unique identifier of the resource
     */
    private String resourceId;

    /**
     * Information about who created the resource
     */
    private CreatedBy createdBy;

    /**
     * Information about with whom the resource is shared with
     */
    private ShareWith shareWith;

    public ResourceSharing(String sourceIdx, String resourceId, CreatedBy createdBy, ShareWith shareWith) {
        this.sourceIdx = sourceIdx;
        this.resourceId = resourceId;
        this.createdBy = createdBy;
        this.shareWith = shareWith;
    }

    public String getSourceIdx() {
        return sourceIdx;
    }

    public void setSourceIdx(String sourceIdx) {
        this.sourceIdx = sourceIdx;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public CreatedBy getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(CreatedBy createdBy) {
        this.createdBy = createdBy;
    }

    public ShareWith getShareWith() {
        return shareWith;
    }

    public void setShareWith(ShareWith shareWith) {
        this.shareWith = shareWith;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceSharing resourceSharing = (ResourceSharing) o;
        return Objects.equals(getSourceIdx(), resourceSharing.getSourceIdx())
            && Objects.equals(getResourceId(), resourceSharing.getResourceId())
            && Objects.equals(getCreatedBy(), resourceSharing.getCreatedBy())
            && Objects.equals(getShareWith(), resourceSharing.getShareWith());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSourceIdx(), getResourceId(), getCreatedBy(), getShareWith());
    }

    @Override
    public String toString() {
        return "Resource {"
            + "sourceIdx='"
            + sourceIdx
            + '\''
            + ", resourceId='"
            + resourceId
            + '\''
            + ", createdBy="
            + createdBy
            + ", sharedWith="
            + shareWith
            + '}';
    }

    @Override
    public String getWriteableName() {
        return "resource_sharing";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceIdx);
        out.writeString(resourceId);
        createdBy.writeTo(out);
        if (shareWith != null) {
            out.writeBoolean(true);
            shareWith.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("source_idx", sourceIdx).field("resource_id", resourceId).field("created_by");
        createdBy.toXContent(builder, params);
        if (shareWith != null && !shareWith.getSharedWithScopes().isEmpty()) {
            builder.field("share_with");
            shareWith.toXContent(builder, params);
        }
        return builder.endObject();
    }

    public static ResourceSharing fromXContent(XContentParser parser) throws IOException {
        String sourceIdx = null;
        String resourceId = null;
        CreatedBy createdBy = null;
        ShareWith shareWith = null;

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                switch (Objects.requireNonNull(currentFieldName)) {
                    case "source_idx":
                        sourceIdx = parser.text();
                        break;
                    case "resource_id":
                        resourceId = parser.text();
                        break;
                    case "created_by":
                        createdBy = CreatedBy.fromXContent(parser);
                        break;
                    case "share_with":
                        shareWith = ShareWith.fromXContent(parser);
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }

        validateRequiredField("source_idx", sourceIdx);
        validateRequiredField("resource_id", resourceId);
        validateRequiredField("created_by", createdBy);

        return new ResourceSharing(sourceIdx, resourceId, createdBy, shareWith);
    }

    private static <T> void validateRequiredField(String field, T value) {
        if (value == null) {
            throw new IllegalArgumentException(field + " is required");
        }
    }
}
