/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.common.annotation.PublicApi;

/**
 * Currently support AND mode.
 * May extend for supporting more modes in the future (OR, IGNORE_IF_EXISTS, etc.).
 */
@PublicApi(since = "1.0.0")
public enum FilterCombinationMode {
    AND,
    /** Unsupported */
    IGNORE_IF_EXISTS,
    /** Unsupported */
    OR
}
