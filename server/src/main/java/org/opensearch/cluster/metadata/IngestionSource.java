/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Objects;

/**
 * Class encapsulating the configuration of an ingestion source.
 */
@ExperimentalApi
public class IngestionSource {
    private String type;
    private String pointerInitReset;
    private Map<String, Object> params;

    public IngestionSource(String type, String pointerInitReset, Map<String, Object> params) {
        this.type = type;
        this.pointerInitReset = pointerInitReset;
        this.params = params;
    }

    public String getType() {
        return type;
    }

    public String getPointerInitReset() {
        return pointerInitReset;
    }

    public Map<String, Object> params() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestionSource ingestionSource = (IngestionSource) o;
        return Objects.equals(type, ingestionSource.type)
            && Objects.equals(pointerInitReset, ingestionSource.pointerInitReset)
            && Objects.equals(params, ingestionSource.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, pointerInitReset, params);
    }

    @Override
    public String toString() {
        return "IngestionSource{" + "type='" + type + '\'' + ",pointer_init_reset='" + pointerInitReset + '\'' + ", params=" + params + '}';
    }
}
