/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serializer for the IDS delegated predicate.
 * Encoding is left to vanilla's {@code IdFieldType.termsQuery} via {@link IdsQueryBuilder#doToQuery}.
 */
public class IdsQuerySerializer extends AbstractQuerySerializer {

    private static final String VALUES_PREFIX = "values.";

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        // Extract id values from indexed MAP keys: MAP('values.0','id0'), MAP('values.1','id1'), ...
        // Starting at operand index 0 (no field operand — the ids query is fieldless).
        Map<String, String> params = ConversionUtils.extractOptionalParams(call, 0);

        // Invariant: every key must be a values.<int> operand — reject unexpected keys loudly
        List<String> invalidKeys = params.keySet().stream().filter(k -> !k.startsWith(VALUES_PREFIX)).collect(Collectors.toList());
        if (!invalidKeys.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "IDS call accepts only values.<int> operands but found: %s", invalidKeys)
            );
        }

        // Collect values in index order for deterministic behaviour
        List<Map.Entry<String, String>> sorted = params.entrySet()
            .stream()
            .sorted((a, b) -> Integer.compare(parseValueIndex(a.getKey()), parseValueIndex(b.getKey())))
            .collect(Collectors.toList());

        // Contiguity check: catches an operand-start-index mismatch silently dropping the first id
        String[] ids = new String[sorted.size()];
        for (int i = 0; i < sorted.size(); i++) {
            int idx = parseValueIndex(sorted.get(i).getKey());
            if (idx != i) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "IDS operand indices must be contiguous 0..%d but got non-contiguous index %d at position %d",
                        sorted.size() - 1,
                        idx,
                        i
                    )
                );
            }
            ids[i] = sorted.get(i).getValue();
        }

        IdsQueryBuilder builder = new IdsQueryBuilder();
        builder.addIds(ids);
        return builder;
    }

    /**
     * Extracts the numeric index suffix from a {@code "values.N"} key.
     *
     * @throws IllegalArgumentException if the suffix is not a valid non-negative integer
     */
    private static int parseValueIndex(String key) {
        String suffix = key.substring(VALUES_PREFIX.length());
        try {
            return Integer.parseInt(suffix);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Malformed operand key [" + key + "]: expected 'values.<int>' but suffix is not an integer",
                e
            );
        }
    }
}
