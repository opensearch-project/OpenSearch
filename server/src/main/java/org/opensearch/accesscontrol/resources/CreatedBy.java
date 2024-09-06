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

/**
 * This class contains information on the creator of a resource.
 * Creator can either be a user or a backend_role.
 *
 * @opensearch.experimental
 */
public class CreatedBy implements ToXContentFragment {

    private String user;

    private String backendRole;

    public CreatedBy(String user, String backendRole) {
        this.user = user;
        this.backendRole = backendRole;
    }

    public String getBackendRole() {
        return backendRole;
    }

    public void setBackendRole(String backendRole) {
        this.backendRole = backendRole;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "CreatedBy {" + "user='" + user + '\'' + ", backendRole='" + backendRole + '\'' + '}';
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("user", user).field("backend_role", backendRole).endObject();
    }
}
