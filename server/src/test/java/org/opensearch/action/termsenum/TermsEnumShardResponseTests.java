/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class TermsEnumShardResponseTests extends AbstractWireSerializingTestCase<TermsEnumShardResponse> {

    @Override
    protected Writeable.Reader<TermsEnumShardResponse> instanceReader() {
        return TermsEnumShardResponse::new;
    }

    @Override
    protected TermsEnumShardResponse createTestInstance() {
        switch (randomIntBetween(0, 3)) {
            case 0:
                return TermsEnumShardResponse.ok(randomTerms(), randomBoolean());
            case 1:
                return TermsEnumShardResponse.skipped();
            case 2:
                return TermsEnumShardResponse.fieldAbsent();
            default:
                return TermsEnumShardResponse.fieldUnsupported(randomAlphaOfLengthBetween(1, 20));
        }
    }

    @Override
    protected TermsEnumShardResponse mutateInstance(TermsEnumShardResponse in) {
        switch (in.outcome()) {
            case OK:
                return randomBoolean()
                    ? TermsEnumShardResponse.ok(in.terms(), in.timedOut() == false)
                    : TermsEnumShardResponse.ok(append(in.terms()), in.timedOut());
            case SKIPPED:
                return TermsEnumShardResponse.fieldAbsent();
            case FIELD_ABSENT:
                return TermsEnumShardResponse.skipped();
            default:
                return TermsEnumShardResponse.fieldUnsupported(in.reason() + "x");
        }
    }

    private static List<String> append(List<String> terms) {
        List<String> copy = new ArrayList<>(terms);
        copy.add("zzz-extra");
        return copy;
    }

    private static List<String> randomTerms() {
        TreeSet<String> set = new TreeSet<>();
        int n = randomIntBetween(0, 10);
        for (int i = 0; i < n; i++) {
            set.add(randomAlphaOfLengthBetween(1, 10));
        }
        return new ArrayList<>(set);
    }

    public void testAccessors() {
        TermsEnumShardResponse ok = TermsEnumShardResponse.ok(List.of("a"), true);
        assertEquals(TermsEnumShardResponse.Outcome.OK, ok.outcome());
        assertTrue(ok.timedOut());
        assertNull(ok.reason());
        TermsEnumShardResponse unsupported = TermsEnumShardResponse.fieldUnsupported("why");
        assertEquals("why", unsupported.reason());
        assertTrue(unsupported.terms().isEmpty());
        assertFalse(unsupported.timedOut());
    }

    public void testValidOrdinals() {
        // the outcome goes between nodes as its ordinal, so reordering or adding values breaks mixed clusters
        assertEquals(0, TermsEnumShardResponse.Outcome.OK.ordinal());
        assertEquals(1, TermsEnumShardResponse.Outcome.SKIPPED.ordinal());
        assertEquals(2, TermsEnumShardResponse.Outcome.FIELD_ABSENT.ordinal());
        assertEquals(3, TermsEnumShardResponse.Outcome.FIELD_UNSUPPORTED.ordinal());
        assertEquals(4, TermsEnumShardResponse.Outcome.values().length);
    }
}
