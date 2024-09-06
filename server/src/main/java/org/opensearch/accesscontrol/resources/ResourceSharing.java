/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A document in .resource_sharing index.
 * Holds information about the resource (obtained from defining plugin's meta-data),
 * the index which defines the resources, the creator of the resource,
 * and the information on whom this resource is shared with.
 *
 * @opensearch.experimental
 */
public class ResourceSharing implements ToXContentFragment {

    private String sourceIdx;

    private String resourceId;

    private CreatedBy createdBy;

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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("source_idx", sourceIdx)
            .field("resource_id", resourceId)
            .field("created_by", createdBy)
            .field("share_with", shareWith)
            .endObject();
    }
}
