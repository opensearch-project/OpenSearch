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
import java.util.Map;

/**
 * Serializer for the IDS delegated predicate. Constructs a vanilla {@link IdsQueryBuilder}
 * and delegates encoding to {@code IdFieldType.termsQuery} (via {@code IdsQueryBuilder.doToQuery}).
 *
 * <p>WHY no {@code Uid.encodeId} here: the serializer hands off to vanilla's {@link IdsQueryBuilder},
 * so encoding happens inside {@code IdFieldType.termsQuery} ({@code IdFieldMapper.java:179}) where
 * it belongs. Coupling the serializer to the internal encoding scheme creates a fragile dependency
 * whose drift produces silently wrong results rather than compile errors.
 */
public class IdsQuerySerializer extends AbstractQuerySerializer {

    private static final String VALUES_PREFIX = "values.";

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        // Extract id values from indexed MAP keys: MAP('values.0','id0'), MAP('values.1','id1'), ...
        // Starting at operand index 0 (no field operand — the ids query is fieldless).
        Map<String, String> params = ConversionUtils.extractOptionalParams(call, 0);

        IdsQueryBuilder builder = new IdsQueryBuilder();
        // Collect values in index order for deterministic behaviour
        String[] ids = params.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith(VALUES_PREFIX))
            .sorted((a, b) -> Integer.compare(parseValueIndex(a.getKey()), parseValueIndex(b.getKey())))
            .map(Map.Entry::getValue)
            .toArray(String[]::new);
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
