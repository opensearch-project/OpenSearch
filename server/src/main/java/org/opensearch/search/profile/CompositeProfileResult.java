/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import java.util.HashMap;
import java.util.Map;

public class CompositeProfileResult {
    private final Map<Class<?>, AbstractProfileResult<?>> results;

    public CompositeProfileResult() {
        this.results = new HashMap<>();
    }

    public void addResult(AbstractProfileResult<?> result) {
        results.put(result.getClass(), result);
    }

    public <R extends AbstractProfileResult<?>> R getResult(Class<R> resultClass) {
        return resultClass.cast(results.get(resultClass));
    }
}
