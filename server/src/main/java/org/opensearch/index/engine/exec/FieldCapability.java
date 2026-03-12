/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Declares what a data format can do with a given field type.
 */
@ExperimentalApi
public enum FieldCapability {
    /** The format can persist raw field values for retrieval (stored fields). */
    STORE,
    /** The format can build an inverted index for search (indexed fields). */
    INDEX,
    /** The format can store columnar data for sorting and aggregations. */
    DOC_VALUES
}
