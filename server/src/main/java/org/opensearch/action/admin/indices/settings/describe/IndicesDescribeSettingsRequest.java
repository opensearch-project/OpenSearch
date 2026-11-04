/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.settings.describe;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.settings.SettingsDescriber;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/** Requests definitions for index setting names, given exactly or as a single trailing-wildcard prefix such as
 * {@code index.routing.allocation.*}. The whole result is returned in one response, in ascending key order.
 * <p>
 * Deliberately not an {@link org.opensearch.action.IndicesRequest}: index setting definitions are node-global (every index
 * on a node is validated against the same registry), so there is no index to name, and an index in the request would be an
 * input the response cannot depend on. Per-index configured values are served by {@code GET /{index}/_settings}.
 * @opensearch.internal
 */
public class IndicesDescribeSettingsRequest extends ActionRequest {
    private final String[] names;

    public IndicesDescribeSettingsRequest(String... names) {
        this.names = names.clone();
    }

    public IndicesDescribeSettingsRequest(StreamInput in) throws IOException {
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
        return SettingsDescriber.validateNames(names, "index.routing.allocation.*");
    }
}
