/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.core.common.bytes.BytesReference;

/**
 * Shared behavior for arrays backed by Java primitive arrays.
 *
 * <p>The concrete classes keep their own primitive array type instead of using a
 * generic base class. Java generics would require boxed values such as {@code Integer},
 * {@code Long}, {@code Float}, and {@code Double}; keeping primitive arrays and
 * type-specific accessors avoids that overhead on indexing paths.</p>
 *
 * <p>The small amount of repeated primitive-specific code is deliberate. Sharing it
 * through generic helpers would require boxed values or indirect callbacks where callers
 * currently use primitive arrays directly.</p>
 */
abstract class AbstractPrimitiveArray {

    private final int dimension;

    AbstractPrimitiveArray(int dimension) {
        this.dimension = dimension;
    }

    public final int dimension() {
        return dimension;
    }

    public final boolean isPackedLE() {
        return false;
    }

    public final BytesReference packedBytes() {
        throw new IllegalStateException("Not packed");
    }
}
