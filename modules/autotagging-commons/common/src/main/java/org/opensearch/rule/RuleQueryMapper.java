/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * This interface is responsible for creating query objects which storage layer can use
 * to query the backend
 * @param <T>
 */
@ExperimentalApi
public interface RuleQueryMapper<T> {
    /**
     * This method translates the {@link GetRuleRequest} to a storage engine specific  query object
     * @param request
     * @return
     */
    T from(GetRuleRequest request);
}
