/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

public class BinarySearcherTests extends RoundableTestCase {
    @Override
    public Roundable newInstance(long[] values, int size) {
        return new BinarySearcher(values, size);
    }
}
