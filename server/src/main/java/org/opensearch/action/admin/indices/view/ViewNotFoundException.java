/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/** Exception thrown when a view is not found */
@ExperimentalApi
public class ViewNotFoundException extends ResourceNotFoundException {

    public ViewNotFoundException(final String viewName) {
        super("View [{}] does not exist", viewName);
    }

    public ViewNotFoundException(final StreamInput in) throws IOException {
        super(in);
    }
}
