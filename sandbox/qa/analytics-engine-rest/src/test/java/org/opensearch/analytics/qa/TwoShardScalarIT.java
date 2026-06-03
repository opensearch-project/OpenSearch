/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.util.Map;

/**
 * 2-shard correctness for per-row scalar functions ({@code scalar/} — round, pow, abs, upper, lower,
 * substring, replace, cast, coalesce, ifnull, isnull, isnotnull, nullif, case, if, date_format + type
 * variations). Scalars are per-row, so this checks the cross-shard gather doesn't corrupt the values.
 */
public class TwoShardScalarIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        return Map.of("scalar", false);
    }
}
