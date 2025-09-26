/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import java.util.List;
import java.util.Map;

public interface ShardResultConvertor {

    List<InternalAggregation> convert(Map<String, Object[]> shardResult);

}
