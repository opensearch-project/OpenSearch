/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import java.util.Objects;

/**
 * A document in .resource_sharing index.
 * Holds information about the resource (obtained from defining plugin's meta-data),
 * the index which defines the resources, the creator of the resource,
 * and the information on whom this resource is shared with.
 *
 * @opensearch.experimental
 */
public class ResourceSharing {

    private String sourceIdx;

    private String resourceId;

    private CreatedBy createdBy;

    private ShareWith sharedWith;

    public ResourceSharing(String sourceIdx, String resourceId, CreatedBy createdBy, ShareWith sharedWith) {
        this.sourceIdx = sourceIdx;
        this.resourceId = resourceId;
        this.createdBy = createdBy;
        this.sharedWith = sharedWith;
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

    public ShareWith getSharedWith() {
        return sharedWith;
    }

    public void setSharedWith(ShareWith sharedWith) {
        this.sharedWith = sharedWith;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceSharing resourceSharing = (ResourceSharing) o;
        return Objects.equals(getSourceIdx(), resourceSharing.getSourceIdx())
            && Objects.equals(getResourceId(), resourceSharing.getResourceId())
            && Objects.equals(getCreatedBy(), resourceSharing.getCreatedBy())
            && Objects.equals(getSharedWith(), resourceSharing.getSharedWith());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSourceIdx(), getResourceId(), getCreatedBy(), getSharedWith());
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
            + sharedWith
            + '}';
    }
}
