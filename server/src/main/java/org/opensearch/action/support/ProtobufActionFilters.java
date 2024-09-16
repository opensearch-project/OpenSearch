/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

/**
 * Holds the action filters injected through plugins, properly sorted by {@link org.opensearch.action.support.ProtobufActionFilter#order()}
*
* @opensearch.internal
*/
public class ProtobufActionFilters {

    private final ProtobufActionFilter[] filters;

    public ProtobufActionFilters(Set<ProtobufActionFilter> actionFilters) {
        this.filters = actionFilters.toArray(new ProtobufActionFilter[0]);
        Arrays.sort(filters, new Comparator<ProtobufActionFilter>() {
            @Override
            public int compare(ProtobufActionFilter o1, ProtobufActionFilter o2) {
                return Integer.compare(o1.order(), o2.order());
            }
        });
    }

    /**
     * Returns the action filters that have been injected
    */
    public ProtobufActionFilter[] filters() {
        return filters;
    }
}
