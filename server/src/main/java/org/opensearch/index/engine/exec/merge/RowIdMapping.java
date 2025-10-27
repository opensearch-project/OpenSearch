/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import java.util.HashMap;
import java.util.Map;

public class RowIdMapping {

    Map<RowId, RowId> mapping = new HashMap<>();

    public RowIdMapping(Map<RowId, RowId> mapping) {
        this.mapping = mapping;
    }

    public RowId getNewRowId(RowId oldRowId) {
        return mapping.get(oldRowId);
    }
}
