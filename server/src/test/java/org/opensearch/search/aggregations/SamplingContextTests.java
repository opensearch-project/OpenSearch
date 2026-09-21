/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;

public class SamplingContextTests extends OpenSearchTestCase {

    public void testScaleUpCount() {
        SamplingContext context = new SamplingContext(0.1);
        assertEquals(1000L, context.scaleUp(100L));
        assertEquals(0L, context.scaleUp(0L));
        // 1 / 0.3 is not an integer, so the scaled count is rounded rather than truncated
        assertEquals(333L, new SamplingContext(0.3).scaleUp(100L));
    }

    public void testScaleUpValue() {
        assertEquals(250.0, new SamplingContext(0.2).scaleUp(50.0), 0.0);
    }

    public void testProbabilityOneScalesNothing() {
        SamplingContext context = new SamplingContext(1.0);
        assertTrue(context.isNoop());
        assertEquals(7L, context.scaleUp(7L));
        assertEquals(7.5, context.scaleUp(7.5), 0.0);
        assertTrue(SamplingContext.NONE.isNoop());
    }

    public void testRejectsProbabilityOutsideZeroToOne() {
        for (double probability : new double[] { 0.0, -0.5, 1.5, Double.NaN }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SamplingContext(probability));
            assertThat(e.getMessage(), containsString("probability"));
        }
    }

    public void testFinalizeSamplingDefaultsToUnchanged() {
        InternalMax max = new InternalMax("max", 42.0, DocValueFormat.RAW, null);
        assertSame(max, max.finalizeSampling(new SamplingContext(0.1)));
    }
}
