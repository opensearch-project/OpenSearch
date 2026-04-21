/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

public class LuceneFieldTrackerTests extends OpenSearchTestCase {

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    /** Creates a {@link FieldInfos} containing the given field names. */
    private static FieldInfos makeFieldInfos(String... names) {
        FieldInfo[] infos = new FieldInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            infos[i] = new FieldInfo(
                names[i],
                i,
                false,
                false,
                false,
                IndexOptions.NONE,
                DocValuesType.NONE,
                DocValuesSkipIndexType.NONE,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
        }
        return new FieldInfos(infos);
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    public void testInitialStateIsEmpty() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        assertSame("initial FieldInfos must be EMPTY", FieldInfos.EMPTY, tracker.getFieldInfos());
        assertEquals(0, tracker.getFieldInfos().size());
    }

    public void testSetAndGetFieldInfosRoundTrip() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        FieldInfos fieldInfos = makeFieldInfos("alpha", "beta", "gamma");
        tracker.setFieldInfos(fieldInfos);
        assertSame(fieldInfos, tracker.getFieldInfos());
    }

    public void testFieldInfosSizeReflectsFieldCount() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        tracker.setFieldInfos(makeFieldInfos("a", "b", "c"));
        assertEquals(3, tracker.getFieldInfos().size());
    }

    public void testFieldInfoLookupReturnsFiForKnownField() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        tracker.setFieldInfos(makeFieldInfos("known_field"));
        assertNotNull(tracker.getFieldInfos().fieldInfo("known_field"));
        assertEquals("known_field", tracker.getFieldInfos().fieldInfo("known_field").name);
    }

    public void testFieldInfoLookupReturnsNullForUnknownField() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        tracker.setFieldInfos(makeFieldInfos("known_field"));
        assertNull(tracker.getFieldInfos().fieldInfo("unknown_field"));
    }

    public void testSetFieldInfosReplacesOldValue() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        tracker.setFieldInfos(makeFieldInfos("old"));
        FieldInfos newInfos = makeFieldInfos("new_a", "new_b");
        tracker.setFieldInfos(newInfos);
        assertSame(newInfos, tracker.getFieldInfos());
        assertEquals(2, tracker.getFieldInfos().size());
        assertNull("old field must no longer be present", tracker.getFieldInfos().fieldInfo("old"));
    }

    public void testSetEmptyFieldInfosClearsFields() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        tracker.setFieldInfos(makeFieldInfos("a", "b"));
        tracker.setFieldInfos(FieldInfos.EMPTY);
        assertEquals(0, tracker.getFieldInfos().size());
    }

    public void testSetFieldInfosWithSingleField() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        tracker.setFieldInfos(makeFieldInfos("only_field"));
        assertEquals(1, tracker.getFieldInfos().size());
        assertNotNull(tracker.getFieldInfos().fieldInfo("only_field"));
    }

    public void testMultipleSetCallsPreservesLastValue() {
        LuceneFieldTracker tracker = new LuceneFieldTracker();
        tracker.setFieldInfos(makeFieldInfos("first"));
        tracker.setFieldInfos(makeFieldInfos("second"));
        tracker.setFieldInfos(makeFieldInfos("third"));
        assertEquals(1, tracker.getFieldInfos().size());
        assertNotNull(tracker.getFieldInfos().fieldInfo("third"));
        assertNull(tracker.getFieldInfos().fieldInfo("first"));
        assertNull(tracker.getFieldInfos().fieldInfo("second"));
    }
}
