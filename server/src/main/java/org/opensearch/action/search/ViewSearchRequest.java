/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ResourceRequest;
import org.opensearch.cluster.metadata.View;
import org.opensearch.common.ValidationException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rest.action.admin.indices.RestViewAction;

/** Wraps the functionality of search requests and tailors for what is available when searching through views 
 */
@ExperimentalApi
public class ViewSearchRequest extends SearchRequest implements ResourceRequest {

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

        validationException = ResourceRequest.validResourceIds(this, validationException);

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
    public Map<String, String> getResourceIds() {
        return Map.of(RestViewAction.VIEW_ID, view.name);
    }
}
