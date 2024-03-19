/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import java.util.function.Supplier;

public class RoundableSupplier implements Supplier<Roundable> {
    private final Supplier<Roundable> delegate;

    RoundableSupplier(String type, long[] values, int size) throws ClassNotFoundException {
        switch (type) {
            case "binary":
                delegate = () -> new BinarySearcher(values, size);
                break;
            case "linear":
                delegate = () -> new BidirectionalLinearSearcher(values, size);
                break;
            case "btree":
                throw new ClassNotFoundException("BtreeSearcher is not supported below JDK 20");
            default:
                throw new IllegalArgumentException("invalid type: " + type);
        }
    }

    @Override
    public Roundable get() {
        return delegate.get();
    }
}
