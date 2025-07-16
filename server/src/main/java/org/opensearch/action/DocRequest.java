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
}
