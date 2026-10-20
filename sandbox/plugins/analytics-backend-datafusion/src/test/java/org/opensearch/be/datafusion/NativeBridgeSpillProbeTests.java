/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the runtime spill-limit probe in {@link NativeBridge}. The probe is
 * forward-compatible: today the upstream DataFusion 53.1.0 {@code DiskManager} does
 * not expose a safe runtime resize through {@code Arc<DiskManager>}, so our Rust
 * crate does not export {@code df_set_spill_limit} and the probe reports
 * {@code false}. When the upstream patch converting {@code max_temp_directory_size}
 * to {@code Arc<AtomicU64>} lands and we ship the symbol, the probe flips to
 * {@code true} automatically.
 *
 * <p>Each test below branches on {@link NativeBridge#isSpillLimitDynamic()} so the
 * suite remains green in both regimes. When the symbol ships, the negative-path
 * assertions become unreachable but the positive-path assertions activate — the
 * test class continues to lock in the expected behaviour without needing rewrites
 * (only verification when the change is made).
 */
public class NativeBridgeSpillProbeTests extends OpenSearchTestCase {

    public void testProbeReportsAvailability() {
        // The probe should return a stable answer for the loaded native library.
        // Today this is false (we do not ship the symbol). When the upstream
        // Arc<AtomicU64> patch lands and our crate exports df_set_spill_limit,
        // this assertion will start passing the other branch.
        boolean dynamic = NativeBridge.isSpillLimitDynamic();
        // Whatever the current value, the probe must be deterministic.
        assertEquals("isSpillLimitDynamic must be deterministic across calls", dynamic, NativeBridge.isSpillLimitDynamic());
    }

    public void testSetSpillLimitContractMatchesProbe() {
        if (NativeBridge.isSpillLimitDynamic()) {
            // Symbol is present — setSpillLimit must not throw the
            // UnsupportedOperationException sentinel. We can't assert on the
            // actual side effect without a live runtime pointer, but invoking the
            // method with runtimePtr=0 must surface a different failure mode
            // (typically a downcall error) rather than the "symbol absent" path.
            try {
                NativeBridge.setSpillLimit(0L, 1024L * 1024 * 1024);
            } catch (UnsupportedOperationException e) {
                fail("isSpillLimitDynamic() returned true but setSpillLimit threw the symbol-absent sentinel: " + e.getMessage());
            } catch (Exception expected) {
                // Any other failure is acceptable here; we're only asserting that
                // the symbol-absent path is not taken when the probe says present.
            }
        } else {
            UnsupportedOperationException e = expectThrows(
                UnsupportedOperationException.class,
                () -> NativeBridge.setSpillLimit(0L, 1024L * 1024 * 1024)
            );
            assertTrue(
                "expected error message to mention the missing symbol, got: " + e.getMessage(),
                e.getMessage().contains("df_set_spill_limit")
            );
        }
    }
}
