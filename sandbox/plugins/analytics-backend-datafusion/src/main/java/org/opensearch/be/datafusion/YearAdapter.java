/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

import java.util.List;

/**
 * Representative {@link AbstractNameMappingAdapter} for Calcite {@code YEAR(ts)}.
 * Rewrites to {@code date_part('year', ts)} so isthmus resolves it against
 * DataFusion's native date_part (see the {@code date_part} signature in
 * {@code opensearch_scalar.yaml}). Demonstrates the reusable rename +
 * literal-arg-injection adapter pattern for cat-3 PPL functions.
 *
 * <p>Follow-up PRs extend the pattern to MONTH/DAY/HOUR/etc. each as a
 * one-line concrete subclass — identical shape, different unit literal.
 *
 * @opensearch.internal
 */
class YearAdapter extends AbstractNameMappingAdapter {

    YearAdapter() {
        super(SqlLibraryOperators.DATE_PART, List.of("year"), List.of());
    }
}
