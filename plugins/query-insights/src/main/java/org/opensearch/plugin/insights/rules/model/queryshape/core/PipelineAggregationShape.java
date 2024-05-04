/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.queryshape.core;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

class PipelineAggregationShape {
    List<String> pipelineAggregations;

    @Override
    public int hashCode() {
        Collections.sort(pipelineAggregations);
        return Objects.hash(pipelineAggregations);
    }
}
