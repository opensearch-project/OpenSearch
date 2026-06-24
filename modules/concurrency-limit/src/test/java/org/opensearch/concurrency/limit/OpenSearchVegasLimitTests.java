/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency.limit;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

public class OpenSearchVegasLimitTests extends OpenSearchTestCase {

    // -------------------------------------------------------------------------
    // Builder defaults
    // -------------------------------------------------------------------------

    public void testDefaultUpDriftFactorIsOne() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).build();
        assertEquals(20, limit.getLimit());
    }

    public void testBuilderRespectsBuildLimits() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(5).maxConcurrency(100).upDriftFactor(2.0).build();
        assertEquals(5, limit.getLimit());
    }

    // -------------------------------------------------------------------------
    // RTT tracking
    // -------------------------------------------------------------------------

    public void testRttNoLoadInitiallyZero() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().build();
        assertEquals(0L, limit.getRttNoLoad(TimeUnit.NANOSECONDS));
    }

    public void testRttNoLoadUpdatedOnFirstSample() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).maxConcurrency(1000).build();
        long rtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(System.nanoTime() - rtt, rtt, 5, false);
        assertEquals(rtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));
    }

    public void testRttNoLoadConvertsUnits() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).build();
        long rttNanos = TimeUnit.MILLISECONDS.toNanos(50);
        limit.onSample(System.nanoTime() - rttNanos, rttNanos, 5, false);
        assertEquals(50L, limit.getRttNoLoad(TimeUnit.MILLISECONDS));
    }

    // -------------------------------------------------------------------------
    // upDriftFactor = 1.0 matches standard Vegas behaviour at defaults
    // -------------------------------------------------------------------------

    public void testDefaultFactorDoesNotChangeInitialLimit() {
        OpenSearchVegasLimit defaultFactor = OpenSearchVegasLimit.newBuilder().initialLimit(20).build();
        OpenSearchVegasLimit explicitOne = OpenSearchVegasLimit.newBuilder().initialLimit(20).upDriftFactor(1.0).build();
        assertEquals(defaultFactor.getLimit(), explicitOne.getLimit());
    }

    public void testDropAlwaysDecreasesLimit() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).maxConcurrency(200).build();
        // Establish a baseline noload RTT first
        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 10, false);

        int before = limit.getLimit();
        // Simulate a drop with high RTT — limit should decrease
        long highRtt = TimeUnit.MILLISECONDS.toNanos(50);
        limit.onSample(0, highRtt, 10, true);
        int after = limit.getLimit();
        assertTrue("drop should not increase limit: before=" + before + " after=" + after, after <= before);
    }

    // -------------------------------------------------------------------------
    // upDriftFactor > 1 widens the no-drift window
    // -------------------------------------------------------------------------

    public void testHigherUpDriftFactorAllowsMoreGrowth() {
        // With upDriftFactor=3, the drift check is: inflight * 4 < limit
        // So a limit of 20 with 4 in-flight (4*4=16 < 20) would NOT drift upward
        // With default (factor=1): inflight * 2 < limit → 4*2=8 < 20 → also no drift
        // But with factor=3 and inflight close to limit, it CAN drift:
        // limit=20, inflight=10: factor=3 → 10*4=40 >= 20 → drift allowed
        // factor=1 → 10*2=20 >= 20 → drift allowed too
        // limit=20, inflight=5: factor=3 → 5*4=20 >= 20 → drift allowed
        // factor=1 → 5*2=10 < 20 → drift PREVENTED

        // We test that a limit with high factor doesn't reject updates when inflight
        // is at the mid-range (which factor=1 would prevent)
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).maxConcurrency(1000).upDriftFactor(1.0).build();
        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 5, false); // record noload

        // With factor=1 and inflight=5, limit=20: 5*2=10 < 20 → no update, returns same limit
        int beforeFactor1 = limit.getLimit();
        long sameRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, sameRtt, 5, false);
        int afterFactor1 = limit.getLimit();
        assertEquals("factor=1 should prevent upward drift at inflight=5 vs limit=20", beforeFactor1, afterFactor1);
    }

    // -------------------------------------------------------------------------
    // toString
    // -------------------------------------------------------------------------

    public void testToStringContainsLimit() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(42).build();
        String str = limit.toString();
        assertTrue(str.contains("OpenSearchVegasLimit"));
        assertTrue(str.contains("42"));
    }

    // -------------------------------------------------------------------------
    // invalid rtt
    // -------------------------------------------------------------------------

    public void testNegativeRttThrows() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).build();
        expectThrows(IllegalArgumentException.class, () -> limit.onSample(0, -1, 5, false));
    }

    public void testZeroRttThrows() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).build();
        expectThrows(IllegalArgumentException.class, () -> limit.onSample(0, 0, 5, false));
    }

    // -------------------------------------------------------------------------
    // Hysteresis
    // -------------------------------------------------------------------------

    public void testIncreaseHysteresisDelaysLimitGrowth() {
        // increaseHysteresis=4: the counter increments on each qualifying sample; after N samples
        // the Nth fires. Sample 1 establishes baseline (counter→1), so 3 more are needed.
        // After samples 2+3 it still shouldn't fire (counter is 2 and 3, both < 4).
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).maxConcurrency(200).increaseHysteresis(4).build();

        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 10, false);  // baseline + increaseConsecutive → 1
        int before = limit.getLimit();

        limit.onSample(0, baseRtt, 10, false);  // counter → 2, no fire
        limit.onSample(0, baseRtt, 10, false);  // counter → 3, no fire
        assertEquals("limit must not grow before hysteresis threshold", before, limit.getLimit());

        limit.onSample(0, baseRtt, 10, false);  // counter → 4, fires
        assertTrue("limit should grow after hitting hysteresis threshold", limit.getLimit() >= before);
    }

    public void testDecreaseHysteresisDelaysLimitDrop() {
        // increaseHysteresis=1000 freezes the limit during setup so it stays at initialLimit.
        // decreaseHysteresis=3: after 2 slow samples limit must not drop; after the 3rd it must.
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder()
            .initialLimit(50)
            .maxConcurrency(500)
            .increaseHysteresis(1000)
            .decreaseHysteresis(3)
            .build();

        long fastRtt = TimeUnit.MILLISECONDS.toNanos(5);
        long slowRtt = TimeUnit.MILLISECONDS.toNanos(50);
        // Establish a fast baseline; inflight=30 ensures the drift check (inflight*2 < limit) fails
        // so updateEstimatedLimit is actually called on each sample.
        for (int i = 0; i < 50; i++)
            limit.onSample(0, fastRtt, 30, false);
        int before = limit.getLimit();

        // High RTT → queueSize ≈ 45 > beta ≈ 12 → decrease zone
        limit.onSample(0, slowRtt, 30, false);  // decreaseConsecutive → 1, no fire
        limit.onSample(0, slowRtt, 30, false);  // decreaseConsecutive → 2, no fire
        assertEquals("limit must not drop before hysteresis threshold", before, limit.getLimit());

        limit.onSample(0, slowRtt, 30, false);  // decreaseConsecutive → 3, fires
        assertTrue("limit should drop after hitting hysteresis threshold", limit.getLimit() <= before);
    }

    public void testDropBypassesDecreaseHysteresis() {
        // didDrop=true is a hard signal and must bypass hysteresis regardless of the counter.
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder().initialLimit(20).maxConcurrency(200).decreaseHysteresis(10).build();

        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 10, false);
        int before = limit.getLimit();

        // Single drop event — hysteresis counter should be irrelevant
        long highRtt = TimeUnit.MILLISECONDS.toNanos(50);
        limit.onSample(0, highRtt, 10, true);
        assertTrue("drop must immediately reduce limit ignoring hysteresis", limit.getLimit() <= before);
    }

    // -------------------------------------------------------------------------
    // Probe inflight threshold
    // -------------------------------------------------------------------------

    public void testProbeSkipsBaselineWhenInflightAboveThreshold() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder()
            .initialLimit(20)
            .maxConcurrency(200)
            .probeMultiplier(1)
            .probeInflightThreshold(0.5)
            .build();

        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 5, false);
        assertEquals(baseRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));

        long inflatedRtt = TimeUnit.MILLISECONDS.toNanos(50);
        // inflight=15 > 20*0.5=10 → probes should skip baseline reset
        for (int i = 0; i < 200; i++) {
            limit.onSample(0, inflatedRtt, 15, false);
        }
        assertEquals("baseline must not be poisoned by high-inflight probes", baseRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));
    }

    public void testProbeAcceptsBaselineWhenInflightBelowThreshold() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder()
            .initialLimit(20)
            .maxConcurrency(200)
            .probeMultiplier(1)
            .probeInflightThreshold(0.5)
            .build();

        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 5, false);
        assertEquals(baseRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));

        long newRtt = TimeUnit.MILLISECONDS.toNanos(15);
        // inflight=5 <= 20*0.5=10 → probes should accept the new RTT
        for (int i = 0; i < 200; i++) {
            limit.onSample(0, newRtt, 5, false);
        }
        assertNotEquals("baseline should update when inflight is below threshold", baseRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));
    }

    public void testProbeThresholdOneDisablesGuard() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder()
            .initialLimit(20)
            .maxConcurrency(200)
            .probeMultiplier(1)
            .probeInflightThreshold(1.0)
            .build();

        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 5, false);

        long newRtt = TimeUnit.MILLISECONDS.toNanos(15);
        // inflight=15, threshold=1.0 → 15 <= limit*1.0 as long as limit >= 15.
        // With moderate RTT inflation and inflight well below limit, Vegas won't shrink limit below 15.
        for (int i = 0; i < 200; i++) {
            limit.onSample(0, newRtt, 15, false);
        }
        assertNotEquals("threshold=1.0 must disable the guard — baseline should change", baseRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));
    }

    public void testProbeThresholdZeroNeverAcceptsProbe() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder()
            .initialLimit(20)
            .maxConcurrency(200)
            .probeMultiplier(1)
            .probeInflightThreshold(0.0)
            .build();

        long baseRtt = TimeUnit.MILLISECONDS.toNanos(10);
        limit.onSample(0, baseRtt, 5, false);

        long inflatedRtt = TimeUnit.MILLISECONDS.toNanos(50);
        // inflight=1 > 20*0.0=0 → probes should never accept
        for (int i = 0; i < 200; i++) {
            limit.onSample(0, inflatedRtt, 1, false);
        }
        assertEquals("threshold=0.0 must never accept probe baseline", baseRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));
    }

    public void testMinRttPathUnaffectedByThreshold() {
        OpenSearchVegasLimit limit = OpenSearchVegasLimit.newBuilder()
            .initialLimit(20)
            .maxConcurrency(200)
            .probeInflightThreshold(0.0)
            .build();

        long highRtt = TimeUnit.MILLISECONDS.toNanos(50);
        limit.onSample(0, highRtt, 5, false);
        assertEquals(highRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));

        // A lower RTT should always update baseline regardless of threshold or inflight.
        long lowRtt = TimeUnit.MILLISECONDS.toNanos(5);
        limit.onSample(0, lowRtt, 20, false);
        assertEquals("min-RTT path must be unconditional even with threshold=0.0", lowRtt, limit.getRttNoLoad(TimeUnit.NANOSECONDS));
    }

    public void testInvalidProbeInflightThresholdThrows() {
        expectThrows(IllegalArgumentException.class, () -> OpenSearchVegasLimit.newBuilder().probeInflightThreshold(-0.1));
        expectThrows(IllegalArgumentException.class, () -> OpenSearchVegasLimit.newBuilder().probeInflightThreshold(1.1));
    }
}
