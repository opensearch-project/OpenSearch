/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.settings.SettingsDescriber;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/** Requests definitions for cluster setting names, given exactly or as a single trailing-wildcard prefix such as
 * {@code cluster.routing.allocation.*}. The whole result is returned in one response, in ascending key order.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsRequest extends ActionRequest {
    private final String[] names;

    public ClusterDescribeSettingsRequest(String... names) {
        this.names = names.clone();
    }

    public ClusterDescribeSettingsRequest(StreamInput in) throws IOException {
        super(in);
        names = in.readStringArray();
    }

    public String[] names() {
        return names.clone();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        return SettingsDescriber.validateNames(names, "cluster.routing.allocation.*");
    }
}
