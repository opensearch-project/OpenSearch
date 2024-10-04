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

import java.io.IOException;
import java.util.List;

/**
 * This class contains information about whom a resource is shared with and at what scope.
 * Here is a sample of what this would look like:
 * "share_with": {
 *       "read_only": {
 *          "users": [],
 *          "roles": [],
 *          "backend_roles": []
 *       },
 *       "read_write": {
 *          "users": [],
 *          "roles": [],
 *          "backend_roles": []
 *       }
 *    }
 *
 * @opensearch.experimental
 */
public class ShareWith implements ToXContentFragment, NamedWriteable {

    private final List<SharedWithScope> sharedWithScopes;

    public ShareWith(List<SharedWithScope> sharedWithScopes) {
        this.sharedWithScopes = sharedWithScopes;
    }

    public List<SharedWithScope> getSharedWithScopes() {
        return sharedWithScopes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject("share_with").value(sharedWithScopes).endObject();
    }

    @Override
    public String getWriteableName() {
        return "share_with";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(sharedWithScopes);
    }
}
