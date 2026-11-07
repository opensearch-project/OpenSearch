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
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.AbstractScopedSettings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Requests definitions for cluster setting names, given exactly or as a single trailing-wildcard prefix such as
 * {@code cluster.routing.allocation.*}. The whole result is returned in one response, in ascending key order.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsRequest extends ActionRequest {
    /**
     * The only admitted wildcard shape: one or more literal dotted segments followed by a trailing {@code .*}. The
     * character class is the one {@link AbstractScopedSettings#isValidKey} accepts, so a prefix is built from the same
     * characters a real key may contain. {@code *}, {@code *.x}, {@code a.*.b}, {@code a.?.*} and {@code a.**} all fail.
     */
    private static final Pattern PREFIX_WILDCARD = Pattern.compile("^(?:[-\\w]+[.])+\\*$");

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
            exception = addValidationError("settings must contain at least one setting name or prefix", exception);
        }
        for (String name : names) {
            if (name == null || name.isBlank()) {
                exception = addValidationError("settings must not contain blank names", exception);
                break;
            }
            boolean wildcard = Regex.isSimpleMatchPattern(name);
            if (wildcard && PREFIX_WILDCARD.matcher(name).matches() == false) {
                exception = addValidationError(
                    "settings may use a single trailing wildcard such as cluster.routing.allocation.*; got [" + name + "]",
                    exception
                );
                break;
            }
            if (wildcard == false && AbstractScopedSettings.isValidKey(name) == false) {
                exception = addValidationError("settings must contain exact setting names; got [" + name + "]", exception);
                break;
            }
        }
        return exception;
    }
}
