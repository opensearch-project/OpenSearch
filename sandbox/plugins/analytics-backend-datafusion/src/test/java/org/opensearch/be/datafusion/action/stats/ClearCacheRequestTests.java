/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Unit tests for {@link ClearCacheNodesRequest} and {@link ClearCacheNodeRequest}.
 * Verifies flag semantics, isClearAll() logic, and round-trip serialization.
 */
public class ClearCacheRequestTests extends OpenSearchTestCase {

    // ── ClearCacheNodesRequest ────────────────────────────────────────────────

    public void testDefaultNodesRequestIsClearAll() {
        ClearCacheNodesRequest req = new ClearCacheNodesRequest();
        assertFalse(req.isFooter());
        assertFalse(req.isColumn());
        assertFalse(req.isOffset());
        assertFalse(req.isStatistics());
        assertTrue("no flags set → isClearAll()", req.isClearAll());
    }

    public void testSetFooterOnlyIsNotClearAll() {
        ClearCacheNodesRequest req = new ClearCacheNodesRequest();
        req.setFooter(true);
        assertTrue(req.isFooter());
        assertFalse(req.isColumn());
        assertFalse(req.isOffset());
        assertFalse(req.isStatistics());
        assertFalse("footer=true → not clear-all", req.isClearAll());
    }

    public void testSetColumnOnlyIsNotClearAll() {
        ClearCacheNodesRequest req = new ClearCacheNodesRequest();
        req.setColumn(true);
        assertFalse(req.isFooter());
        assertTrue(req.isColumn());
        assertFalse(req.isOffset());
        assertFalse(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testSetOffsetOnlyIsNotClearAll() {
        ClearCacheNodesRequest req = new ClearCacheNodesRequest();
        req.setOffset(true);
        assertFalse(req.isFooter());
        assertFalse(req.isColumn());
        assertTrue(req.isOffset());
        assertFalse(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testSetStatisticsOnlyIsNotClearAll() {
        ClearCacheNodesRequest req = new ClearCacheNodesRequest();
        req.setStatistics(true);
        assertFalse(req.isFooter());
        assertFalse(req.isColumn());
        assertFalse(req.isOffset());
        assertTrue(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testAllFlagsSetIsNotClearAll() {
        ClearCacheNodesRequest req = new ClearCacheNodesRequest();
        req.setFooter(true);
        req.setColumn(true);
        req.setOffset(true);
        req.setStatistics(true);
        assertFalse(req.isClearAll());
    }

    public void testNodesRequestRoundTrip() throws IOException {
        ClearCacheNodesRequest original = new ClearCacheNodesRequest();
        original.setFooter(true);
        original.setColumn(false);
        original.setOffset(true);
        original.setStatistics(true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ClearCacheNodesRequest deserialized = new ClearCacheNodesRequest(in);

        assertEquals(original.isFooter(), deserialized.isFooter());
        assertEquals(original.isColumn(), deserialized.isColumn());
        assertEquals(original.isOffset(), deserialized.isOffset());
        assertEquals(original.isStatistics(), deserialized.isStatistics());
        assertEquals(original.isClearAll(), deserialized.isClearAll());
    }

    // ── ClearCacheNodeRequest ─────────────────────────────────────────────────

    public void testNodeRequestIsClearAllWhenNoFlagsSet() {
        ClearCacheNodeRequest req = new ClearCacheNodeRequest(false, false, false, false);
        assertTrue(req.isClearAll());
    }

    public void testNodeRequestIsNotClearAllWhenFlagSet() {
        ClearCacheNodeRequest req = new ClearCacheNodeRequest(true, false, false, false);
        assertFalse(req.isClearAll());
    }

    public void testNodeRequestStatisticsFlag() {
        ClearCacheNodeRequest req = new ClearCacheNodeRequest(false, false, false, true);
        assertFalse(req.isFooter());
        assertFalse(req.isColumn());
        assertFalse(req.isOffset());
        assertTrue(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testNodeRequestRoundTrip() throws IOException {
        ClearCacheNodeRequest original = new ClearCacheNodeRequest(false, true, true, true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ClearCacheNodeRequest deserialized = new ClearCacheNodeRequest(in);

        assertFalse(deserialized.isFooter());
        assertTrue(deserialized.isColumn());
        assertTrue(deserialized.isOffset());
        assertTrue(deserialized.isStatistics());
        assertFalse(deserialized.isClearAll());
    }
}
