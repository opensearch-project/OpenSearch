/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeProfileBreakdown {
    private final Map<Class<?>, AbstractProfileBreakdown<?>> breakdowns;
    private final Map<AbstractProfileBreakdown<?>, AbstractProfileResult<?>> results;

    public CompositeProfileBreakdown() {
        this.breakdowns = new HashMap<>();
        this.results = new HashMap<>();
    }

    public CompositeProfileBreakdown(List<Class<? extends AbstractProfileBreakdown<?>>> breakdownClasses, List<Class<? extends AbstractProfileResult<?>>> resultClasses) throws Exception {
        this.breakdowns = new HashMap<>();
        this.results = new HashMap<>();
        for(int i = 0; i < breakdownClasses.size(); i++) {
            AbstractProfileBreakdown<?> breakdown = breakdownClasses.get(i).getDeclaredConstructor().newInstance();
            AbstractProfileResult<?> result = resultClasses.get(i).getDeclaredConstructor().newInstance();
            addBreakdown(breakdown, result);
        }
    }

    public void addBreakdown(AbstractProfileBreakdown<?> breakdown, AbstractProfileResult<?> result) {
        breakdowns.put(breakdown.getClass(), breakdown);
        results.put(breakdown, result);
    }

    public Map<Class<?>, AbstractProfileBreakdown<?>> getBreakdowns() {
        return breakdowns;
    }

    public Map<AbstractProfileBreakdown<?>, AbstractProfileResult<?>> getResults() {
        return results;
    }

    public <B extends AbstractProfileBreakdown<?>> B getBreakdown(Class<B> breakdownClass) {
        return breakdownClass.cast(breakdowns.get(breakdownClass));
    }
}
