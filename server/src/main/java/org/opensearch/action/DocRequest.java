/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.common.annotation.PublicApi;

/**
 * Generic interface to group ActionRequest, which perform actions on a single document
 *
 * @opensearch.api
 */
@PublicApi(since = "3.1.0")
public interface DocRequest {
    /**
     * Get the index that this request operates on
     * @return the index
     */
    String index();

    /**
     * Get the id of the document for this request
     * @return the id
     */
    String id();

    /**
     * Get the type of the request. This should match the action name prefix: i.e. indices:data/read/get
     *
     * Used in the context of resource sharing to specify the type of sharable resource.
     *
     * i.e. A report definition is sharable, so ActionRequests in the reporting plugin that
     * pertain to a single report definition would override this to specify "report_definition"
     *
     * @return the type
     */
    default String type() {
        return "indices";
    }
}
