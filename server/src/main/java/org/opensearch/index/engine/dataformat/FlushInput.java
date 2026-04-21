/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Parameter object for {@link Writer#flush(FlushInput)}.
 * Carries optional context that writers may use during flush.
 * Writers that don't need any context simply ignore the fields.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record FlushInput(long[][] sortPermutation) {

    /** Empty flush input with no sort permutation. */
    public static final FlushInput EMPTY = new FlushInput(null);

    /**
     * Returns whether a sort permutation is available.
     */
    public boolean hasSortPermutation() {
        return sortPermutation != null && sortPermutation.length == 2 && sortPermutation[0].length > 0;
    }
}
