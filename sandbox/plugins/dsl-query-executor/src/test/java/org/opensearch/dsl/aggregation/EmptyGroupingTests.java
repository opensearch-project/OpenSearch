/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.test.OpenSearchTestCase;

public class EmptyGroupingTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testFieldNamesIsEmpty() {
        EmptyGrouping grouping = new EmptyGrouping();

        assertTrue(grouping.getFieldNames().isEmpty());
    }

    public void testResolveIndicesIsEmpty() throws ConversionException {
        EmptyGrouping grouping = new EmptyGrouping();

        assertTrue(grouping.resolveIndices(ctx.getRowType()).isEmpty());
    }
}
