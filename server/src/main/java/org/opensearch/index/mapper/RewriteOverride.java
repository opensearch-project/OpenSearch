/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.PublicApi;

/** A custom rewrite override for a query. Default executes the query as is and other values determine which structure to use at run-time.
*
* @opensearch.internal
*/
@PublicApi(since = "2.17.0")
public enum RewriteOverride {

    // don't override the rewrite, use default
    DEFAULT,

    // use index structure
    INDEX_ONLY,

    // use doc_values structure
    DOC_VALUES_ONLY
}
