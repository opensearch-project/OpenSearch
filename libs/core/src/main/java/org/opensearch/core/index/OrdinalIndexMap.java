/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.index;

import java.util.HashMap;
import java.util.Map;

public class OrdinalIndexMap {

    public static final OrdinalIndexMap INSTANCE = new OrdinalIndexMap();

    private Map<Integer,String> lookupMap = new HashMap<>();

    public static OrdinalIndexMap getInstance() {
        return INSTANCE;
    }

    public void initOrdinalIndexMap() {

    }

    public void updateOrdinalIndexMap(int index, String ordinalIndex) {
        lookupMap.put(index, ordinalIndex);
    }

    public String getOrdinalIndex(int index) {
        return lookupMap.get(index);
    }

}
