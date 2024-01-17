/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.View;
import org.opensearch.common.at org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rest.action.admin.indicport java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Wraps the functionality of search requests and tailors for what is available when searching through views
 */
@ExperimentalApi
public class ViewSearchRequest extends SearchRequest {

    public final View view;

    public ViewSearchRequest(final View view) {
        super();
        this.view = view;
    }

    public ViewSearchRequest(final StreamInput in) throws IOException {
        super(in);
        view = new View(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        final Function<String, String> unsupported = (String x) -> x + " is not supported when searching views";
        ActionRequestValidationException validationException = super.validate();

        if (scroll() != null) {
            validationException = addValidationError(unsupported.apply("Scroll"), validationException);
        }

        // TODO: Filter out anything additional search features that are not supported

        return validationException;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        view.writeTo(out);
    }

    @Override
    public boolean equals(final Object o) {
        // TODO: Maybe this isn't standard practice
        return this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(view, super.hashCode());
    }

    @Override
    public String toString() {
        return super.toString().replace("SearchRequest{", "ViewSearchRequest{view=" + view + ",");
    }

    @Override
    public Map<String, String> getResourceTypeAndIds() {
        return Map.of(RestViewAction.VIEW_ID, view.name);
    }
}
