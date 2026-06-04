/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.index.MergePolicy;
import org.opensearch.common.annotation.InternalApi;

/**
 * A provider for obtaining merge policies used by OpenSearch indexes.
 *
 * @opensearch.internal
 */

@InternalApi
public interface MergePolicyProvider {
    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    String INDEX_MERGE_ENABLED = "index.merge.enabled";

    /**
     * Gets the merge policy to be used for index.
     *
     * @return The merge policy instance.
     */
    MergePolicy getMergePolicy();
}
