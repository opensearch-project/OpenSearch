/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link ObjectSlice} covering construction validation, accessors, emptiness, and value semantics.
 */
public class ObjectSliceTests extends OpenSearchTestCase {

    public void testAccessors() {
        ObjectSlice slice = new ObjectSlice("obj", 2, 10, 12);
        assertThat(slice.getObjectName(), equalTo("obj"));
        assertThat(slice.getStartOffset(), is(2L));
        assertThat(slice.getEndOffsetExclusive(), is(10L));
        assertThat(slice.getObjectSize(), is(12L));
    }

    public void testEndsAtObjectEndOnlyWhenTheEndEqualsTheDeclaredSize() {
        assertThat(new ObjectSlice("obj", 2, 12, 12).endsAtObjectEnd(), is(true));
        assertThat(new ObjectSlice("obj", 0, 0, 0).endsAtObjectEnd(), is(true));
        assertThat(new ObjectSlice("obj", 2, 10, 12).endsAtObjectEnd(), is(false));
    }

    public void testObjectSizeBeforeEndThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectSlice("obj", 0, 5, 4));
        assertThat(e.getMessage(), containsString("objectSize [4] must not be less than endOffsetExclusive [5]"));
    }

    public void testNullObjectNameThrows() {
        expectThrows(NullPointerException.class, () -> new ObjectSlice(null, 0, 1, 1));
    }

    public void testNegativeStartOffsetThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectSlice("obj", -1, 1, 1));
        assertThat(e.getMessage(), containsString("startOffset must not be negative"));
    }

    public void testEndBeforeStartThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectSlice("obj", 5, 4, 4));
        assertThat(e.getMessage(), containsString("must not be less than startOffset"));
    }

    public void testEqualsSameInstance() {
        ObjectSlice slice = new ObjectSlice("obj", 0, 1, 1);
        assertThat(slice.equals(slice), is(true));
    }

    public void testEqualsNull() {
        assertThat(new ObjectSlice("obj", 0, 1, 1).equals(null), is(false));
    }

    public void testEqualsDifferentType() {
        assertThat(new ObjectSlice("obj", 0, 1, 1).equals("obj"), is(false));
    }

    public void testEqualsAndHashCodeForEqualSlices() {
        ObjectSlice a = new ObjectSlice("obj", 1, 4, 4);
        ObjectSlice b = new ObjectSlice("obj", 1, 4, 4);
        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), is(b.hashCode()));
    }

    public void testNotEqualsDifferingFields() {
        ObjectSlice base = new ObjectSlice("obj", 1, 4, 4);
        assertThat(base, not(equalTo(new ObjectSlice("other", 1, 4, 4))));
        assertThat(base, not(equalTo(new ObjectSlice("obj", 2, 4, 4))));
        assertThat(base, not(equalTo(new ObjectSlice("obj", 1, 5, 5))));
        assertThat(base, not(equalTo(new ObjectSlice("obj", 1, 4, 5))));
    }

    public void testToStringContainsFields() {
        String s = new ObjectSlice("obj", 1, 4, 4).toString();
        assertThat(s, containsString("objectName='obj'"));
        assertThat(s, containsString("startOffset=1"));
        assertThat(s, containsString("endOffsetExclusive=4"));
        assertThat(s, containsString("objectSize=4"));
    }
}
