/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import java.util.List;

import jdk.incubator.vector.LongVector;

public class BtreeSearcherTests extends RoundableTestCase {
    @Override
    public Roundable newInstance(long[] values, int size) {
        return new BtreeSearcher(values, size, randomFrom(List.of(LongVector.SPECIES_128, LongVector.SPECIES_256, LongVector.SPECIES_512)));
    }
}
