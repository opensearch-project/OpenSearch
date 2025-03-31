/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

public class BytesRefsCollectionBuilderTests extends OpenSearchTestCase {

    public void testBuildSortedNotSorted() {
        String[] seedStrings = generateRandomStringArray(10, 10, false, true);
        List<BytesRef> bytesRefList = Arrays.stream(seedStrings).map(BytesRef::new).toList();
        List<BytesRef> sortedBytesRefs = bytesRefList.stream().sorted().toList();

        Collection<BytesRef> sortedSet = assertCollectionBuilt(sortedBytesRefs);
        assertCollectionBuilt(bytesRefList);

        assertTrue(sortedSet instanceof SortedSet<BytesRef>);
        assertTrue(((SortedSet<BytesRef>) sortedSet).comparator() == null);
    }

    public void testBuildFooBar() {
        String[] reverseOrderStrings = new String[] { "foo", "bar" };
        List<BytesRef> bytesRefList = Arrays.stream(reverseOrderStrings).map(BytesRef::new).toList();
        List<BytesRef> sortedBytesRefs = bytesRefList.stream().sorted().toList();

        Collection<BytesRef> sortedSet = assertCollectionBuilt(sortedBytesRefs);
        Collection<BytesRef> reverseList = assertCollectionBuilt(bytesRefList);

        assertTrue(sortedSet instanceof SortedSet<BytesRef>);
        assertTrue(((SortedSet<BytesRef>) sortedSet).comparator() == null);

        assertTrue(reverseList instanceof List<BytesRef>);
    }

    public void testFrozen() {
        BytesRefsCollectionBuilder builder = new BytesRefsCollectionBuilder();
        String[] seedStrings = generateRandomStringArray(5, 10, false, true);
        Arrays.stream(seedStrings).map(BytesRef::new).forEachOrdered(builder);
        Collection<BytesRef> bytesRefCollection = builder.get();
        assertNotNull(bytesRefCollection);
        assertEquals(seedStrings.length, bytesRefCollection.size());
        assertThrows(IllegalStateException.class, () -> builder.accept(new BytesRef("illegal state")));
    }

    private static Collection<BytesRef> assertCollectionBuilt(List<BytesRef> sortedBytesRefs) {
        BytesRefsCollectionBuilder builder = new BytesRefsCollectionBuilder();
        sortedBytesRefs.stream().forEachOrdered(builder);
        Collection<BytesRef> bytesRefCollection = builder.get();
        assertEquals(bytesRefCollection.size(), sortedBytesRefs.size());
        for (Iterator<BytesRef> iterator = bytesRefCollection.iterator(), iterator2 = sortedBytesRefs.iterator(); iterator.hasNext()
            || iterator2.hasNext();) {
            assertTrue(iterator.next().bytesEquals(iterator2.next()));
        }
        return bytesRefCollection;
    }
}
