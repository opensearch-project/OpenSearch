/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.settings.AbstractScopedSettings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Requests definitions for exact cluster setting names.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsRequest extends ClusterManagerNodeRequest<ClusterDescribeSettingsRequest> {
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
        ActionRequestValidationException exception = null;
        if (names.length == 0) {
            exception = addValidationError("settings must contain at least one exact setting name", exception);
        }
        for (String name : names) {
            if (name == null
                || name.isBlank()
                || name.contains("*")
                || name.contains("?")
                || AbstractScopedSettings.isValidKey(name) == false) {
                exception = addValidationError("settings must contain exact setting names, without wildcards", exception);
                break;
            }
        }
        return exception;
    }
}
