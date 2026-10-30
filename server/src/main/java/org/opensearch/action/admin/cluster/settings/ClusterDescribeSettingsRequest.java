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
import org.opensearch.action.pagination.PageParams;
import org.opensearch.common.Nullable;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.AbstractScopedSettings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Requests definitions for cluster setting names, given exactly or as a single trailing-wildcard prefix such as
 * {@code cluster.routing.allocation.*}. Results are always paginated; without explicit page parameters the first
 * {@link #DEFAULT_PAGE_SIZE} keys are returned in ascending order.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsRequest extends ActionRequest {
    /** Page bounds follow the other core listing APIs ({@code _list/indices}, {@code _list/shards}). */
    public static final int DEFAULT_PAGE_SIZE = 100;
    public static final int MAX_PAGE_SIZE = 1000;
    /**
     * The only admitted wildcard shape: one or more literal dotted segments followed by a trailing {@code .*}. The
     * character class is the one {@link AbstractScopedSettings#isValidKey} accepts, so a prefix is built from the same
     * characters a real key may contain. {@code *}, {@code *.x}, {@code a.*.b}, {@code a.?} and {@code a.**} all fail.
     */
    private static final Pattern PREFIX_WILDCARD = Pattern.compile("^(?:[-\\w]+[.])+\\*$");

    private final String[] names;
    @Nullable
    private final PageParams pageParams;

    public ClusterDescribeSettingsRequest(String... names) {
        this(null, names);
    }

    public ClusterDescribeSettingsRequest(@Nullable PageParams pageParams, String... names) {
        this.names = names.clone();
        this.pageParams = pageParams;
    }

    public ClusterDescribeSettingsRequest(StreamInput in) throws IOException {
        super(in);
        names = in.readStringArray();
        pageParams = in.readOptionalWriteable(PageParams::new);
    }

    public String[] names() {
        return names.clone();
    }

    /** Page parameters, or {@code null} to return the first {@link #DEFAULT_PAGE_SIZE} keys in ascending order. */
    @Nullable
    public PageParams pageParams() {
        return pageParams;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
        out.writeOptionalWriteable(pageParams);
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
            if (Regex.isSimpleMatchPattern(name) || name.contains("?")) {
                if (PREFIX_WILDCARD.matcher(name).matches() == false) {
                    exception = addValidationError(
                        "settings may use a single trailing wildcard such as cluster.routing.allocation.*; got [" + name + "]",
                        exception
                    );
                    break;
                }
            } else if (AbstractScopedSettings.isValidKey(name) == false) {
                exception = addValidationError("settings must contain exact setting names; got [" + name + "]", exception);
                break;
            }
        }
        if (pageParams != null) {
            if (pageParams.getSize() < 1 || pageParams.getSize() > MAX_PAGE_SIZE) {
                exception = addValidationError("size must be between 1 and " + MAX_PAGE_SIZE, exception);
            }
            if (PageParams.PARAM_ASC_SORT_VALUE.equals(pageParams.getSort()) == false
                && PageParams.PARAM_DESC_SORT_VALUE.equals(pageParams.getSort()) == false) {
                exception = addValidationError("sort must be asc or desc", exception);
            }
        }
        return exception;
    }
}
