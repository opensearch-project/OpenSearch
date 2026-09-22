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
 * Locks in the return contract of {@code df_can_match}: every status it reports must be
 * non-negative.
 *
 * <p>{@code #[ffm_safe]} returns {@code Err} as a negated pointer to a heap-allocated message, so
 * {@code NativeLibraryLoader.checkResult} reads any negative return as an address. A negative
 * status is therefore dereferenced as an error string and segfaults the JVM — it does not fail the
 * query, it kills the node. This ran on the can-match fail-open path, so the safety valve was the
 * crash.
 *
 * <p>A null shard view reaches a non-match status without a live runtime or parquet on disk, which
 * makes the contract testable without fixtures.
 */
public class NativeBridgeCanMatchTests extends OpenSearchTestCase {

    private static final String COLUMN = "@timestamp";

    /** Before the fix this segfaulted the test JVM rather than returning. */
    public void testNullShardViewReturnsUnknownRatherThanNegative() {
        long rc = NativeBridge.canMatch(0L, 0L, COLUMN, 0L, 100L);

        assertTrue("negative returns are error pointers to checkResult, not statuses; got " + rc, rc >= 0);
        assertEquals(NativeBridge.CAN_MATCH_UNKNOWN, rc);
    }

    /** UNKNOWN must not collide with NO, or a fail-open answer would silently prune the shard. */
    public void testUnknownIsDistinctFromNo() {
        assertNotEquals(NativeBridge.CAN_MATCH_NO, NativeBridge.CAN_MATCH_UNKNOWN);
        assertTrue(NativeBridge.CAN_MATCH_NO >= 0);
        assertTrue(NativeBridge.CAN_MATCH_YES >= 0);
        assertTrue(NativeBridge.CAN_MATCH_UNKNOWN >= 0);
    }
}
