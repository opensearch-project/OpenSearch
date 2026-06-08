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
 * substring, replace, cast, coalesce, ifnull, isnull, isnotnull, nullif, case, if, date_format, in +
 * type variations). Scalars are per-row, so this checks the cross-shard gather doesn't corrupt the
 * values; every query is {@code sort}-ed for a deterministic order across the 2-shard gather.
 */
public class TwoShardScalarIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        return Map.of("scalar", false);
    }

    @Override
    protected Map<String, String> knownIssues() {
        // coalesce() on an ip column throws "unsupported object class [B" (ip byte[] not handled).
        return Map.of("sc_coalesce_ip", "coalesce() on ip throws ExpressionEvaluationException: unsupported object class [B");
    }
}
