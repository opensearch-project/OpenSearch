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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Stream;

public class BytesRefsCollectionBuilderTests extends OpenSearchTestCase {

    public void testBuildSortedNotSorted() {
        String[] seedStrings = generateRandomStringArray(10, 10, false, true);
        List<BytesRef> bytesRefList = Arrays.stream(seedStrings).map(BytesRef::new).toList();
        List<BytesRef> sortedBytesRefs = bytesRefList.stream().sorted().toList();

        Collection<BytesRef> sortedSet = assertCollectionBuilt(sortedBytesRefs);
        assertCollectionBuilt(bytesRefList);

        assertTrue(sortedSet.isEmpty() || sortedSet instanceof SortedSet<BytesRef>);
        if (!sortedSet.isEmpty()) {
            assertNull(((SortedSet<BytesRef>) sortedSet).comparator());
        }
    }

    public void testBuildFooBar() {
        String[] reverseOrderStrings = new String[] { "foo", "bar" };
        List<BytesRef> bytesRefList = Arrays.stream(reverseOrderStrings).map(BytesRef::new).toList();
        List<BytesRef> sortedBytesRefs = bytesRefList.stream().sorted().toList();

        Collection<BytesRef> sortedSet = assertCollectionBuilt(sortedBytesRefs);
        Collection<BytesRef> reverseList = assertCollectionBuilt(bytesRefList);

        assertTrue(sortedSet instanceof SortedSet<BytesRef>);
        assertNull(((SortedSet<BytesRef>) sortedSet).comparator());

        assertTrue(reverseList instanceof List<BytesRef>);
    }

    public void testFrozen() {
        BytesRefsCollectionBuilder builder = new BytesRefsCollectionBuilder(1);
        String[] seedStrings = generateRandomStringArray(5, 10, false, true);
        Arrays.stream(seedStrings).map(BytesRef::new).forEachOrdered(builder);
        Collection<BytesRef> bytesRefCollection = builder.get();
        assertNotNull(bytesRefCollection);
        assertEquals(seedStrings.length, bytesRefCollection.size());
        assertThrows(IllegalStateException.class, () -> builder.accept(new BytesRef("illegal state")));
        assertSame(bytesRefCollection, builder.get());
    }

    private static Collection<BytesRef> assertCollectionBuilt(List<BytesRef> sortedBytesRefs) {
        BytesRefsCollectionBuilder builder = new BytesRefsCollectionBuilder(1);
        sortedBytesRefs.stream().forEachOrdered(builder);
        Collection<BytesRef> bytesRefCollection = builder.get();
        assertEquals(bytesRefCollection.size(), sortedBytesRefs.size());
        for (Iterator<BytesRef> iterator = bytesRefCollection.iterator(), iterator2 = sortedBytesRefs.iterator(); iterator.hasNext()
            || iterator2.hasNext();) {
            assertTrue(iterator.next().bytesEquals(iterator2.next()));
        }
        return bytesRefCollection;
    }

    public void testCoverUnsupported() {
        BytesRefsCollectionBuilder builder = new BytesRefsCollectionBuilder(1);
        Stream.of("in", "order").map(BytesRef::new).forEachOrdered(builder);
        SortedSet<BytesRef> bytesRefCollection = (SortedSet<BytesRef>) builder.get();
        assertThrows(UnsupportedOperationException.class, () -> bytesRefCollection.subSet(new BytesRef("a"), new BytesRef("z")));
        assertThrows(UnsupportedOperationException.class, () -> bytesRefCollection.headSet(new BytesRef("a")));
        assertThrows(UnsupportedOperationException.class, () -> bytesRefCollection.tailSet(new BytesRef("a")));
        assertThrows(UnsupportedOperationException.class, bytesRefCollection::first);
        assertThrows(UnsupportedOperationException.class, bytesRefCollection::last);
    }
}
