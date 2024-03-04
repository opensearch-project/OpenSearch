/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.keystore;

import org.opensearch.common.Randomness;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.roaringbitmap.RoaringBitmap;

public class RBMIntKeyLookupStoreTests extends OpenSearchTestCase {

    final int BYTES_IN_MB = 1048576;

    public void testInit() {
        long memCap = 100 * BYTES_IN_MB;
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(memCap);
        assertEquals(0, kls.getSize());
        assertEquals(RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_EIGHT.getValue(), kls.modulo);
        assertEquals(memCap, kls.getMemorySizeCapInBytes());
    }

    public void testTransformationLogic() throws Exception {
        int modulo = (int) Math.pow(2, 29);
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_NINE, 0L);
        int offset = 3;
        for (int i = 0; i < 4; i++) { // after this we run into max value, but thats not a flaw with the class design
            int posValue = i * modulo + offset;
            kls.add(posValue);
            assertEquals(offset, (int) kls.getInternalRepresentation(posValue));
            int negValue = -(i * modulo + offset);
            kls.add(negValue);
            assertEquals(modulo - offset, (int) kls.getInternalRepresentation(negValue));
        }
        assertEquals(2, kls.getSize());
        int[] testVals = new int[] { 0, 1, -1, -23495, 23058, modulo, -modulo, Integer.MAX_VALUE, Integer.MIN_VALUE };
        for (int value : testVals) {
            assertTrue(kls.getInternalRepresentation(value) < modulo);
            assertTrue(kls.getInternalRepresentation(value) >= 0);
        }
        RBMIntKeyLookupStore no_modulo_kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.NONE, 0L);
        Random rand = Randomness.get();
        for (int i = 0; i < 100; i++) {
            int val = rand.nextInt();
            assertEquals(val, (int) no_modulo_kls.getInternalRepresentation(val));
        }
    }

    public void testContains() throws Exception {
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_NINE, 0L);
        RBMIntKeyLookupStore noModuloKls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.NONE, 0L);
        for (int i = 0; i < RBMIntKeyLookupStore.REFRESH_SIZE_EST_INTERVAL + 1000; i++) {
            // set upper bound > number of elements to trigger a size check, ensuring we test that too
            kls.add(i);
            assertTrue(kls.contains(i));
            noModuloKls.add(i);
            assertTrue(noModuloKls.contains(i));
        }
    }

    public void testAddingStatsGetters() throws Exception {
        RBMIntKeyLookupStore.KeystoreModuloValue moduloValue = RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_SIX;
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(moduloValue, 0L);
        kls.add(15);
        kls.add(-15);
        assertEquals(2, kls.getAddAttempts());
        assertEquals(0, kls.getCollisions());

        int offset = 1;
        for (int i = 0; i < 10; i++) {
            kls.add(i * moduloValue.getValue() + offset);
        }
        assertEquals(12, kls.getAddAttempts());
        assertEquals(9, kls.getCollisions());
    }

    public void testRegenerateStore() throws Exception {
        int numToAdd = 10000000;
        Random rand = Randomness.get();
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_NINE, 0L);
        for (int i = 0; i < numToAdd; i++) {
            kls.add(i);
        }
        assertEquals(numToAdd, kls.getSize());
        Integer[] newVals = new Integer[1000]; // margin accounts for collisions
        for (int j = 0; j < newVals.length; j++) {
            newVals[j] = rand.nextInt();
        }
        kls.regenerateStore(newVals);
        assertTrue(Math.abs(kls.getSize() - newVals.length) < 3); // inexact due to collisions

        // test clear()
        kls.clear();
        assertEquals(0, kls.getSize());
    }

    public void testAddingDuplicates() throws Exception {
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(0L);
        int numToAdd = 4820411;
        for (int i = 0; i < numToAdd; i++) {
            kls.add(i);
            kls.add(i);
        }
        for (int j = 0; j < 1000; j++) {
            kls.add(577);
        }
        assertEquals(numToAdd, kls.getSize());
    }

    public void testMemoryCapBlocksAdd() throws Exception {
        // Now that we're using a modified version of rbm.getSizeInBytes(), which doesn't provide an inverse function,
        // we have to test filling just an RBM with random test values first so that we can get the resulting memory cap limit
        // to use with our modified size estimate.
        // This is much noisier so the precision is lower.

        // It is necessary to use randomly distributed integers for both parts of this test, as we would do with hashes in the cache,
        // as that's what our size estimator is designed for.
        // If we add a run of integers, our size estimator is not valid, especially for small RBMs.

        int[] maxEntriesArr = new int[] { 1342000, 100000, 3000000 };
        long[] rbmReportedSizes = new long[4];
        Random rand = Randomness.get();
        for (int j = 0; j < maxEntriesArr.length; j++) {
            RoaringBitmap rbm = new RoaringBitmap();
            for (int i = 0; i < maxEntriesArr[j]; i++) {
                rbm.add(rand.nextInt());
            }
            rbmReportedSizes[j] = rbm.getSizeInBytes();
        }
        RBMIntKeyLookupStore.KeystoreModuloValue moduloValue = RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_NINE;
        for (int i = 0; i < maxEntriesArr.length; i++) {
            double multiplier = RBMIntKeyLookupStore.getRBMSizeMultiplier(maxEntriesArr[i], moduloValue.getValue());
            long memSizeCapInBytes = (long) (rbmReportedSizes[i] * multiplier);
            RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(moduloValue, memSizeCapInBytes);
            for (int j = 0; j < maxEntriesArr[i] + 5000; j++) {
                kls.add(rand.nextInt());
            }
            assertTrue(Math.abs(maxEntriesArr[i] - kls.getSize()) < (double) maxEntriesArr[i] / 10);
        }
    }

    public void testConcurrency() throws Exception {
        Random rand = Randomness.get();
        for (int j = 0; j < 5; j++) { // test with different numbers of threads
            RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_NINE, 0L);
            int numThreads = rand.nextInt(50) + 1;
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
            // In this test we want to add the first 200K numbers and check they're all correctly there.
            // We do some duplicates too to ensure those aren't incorrectly added.
            int amountToAdd = 200000;
            ArrayList<Future<Boolean>> wasAdded = new ArrayList<>(amountToAdd);
            ArrayList<Future<Boolean>> duplicatesWasAdded = new ArrayList<>();
            for (int i = 0; i < amountToAdd; i++) {
                wasAdded.add(null);
            }
            for (int i = 0; i < amountToAdd; i++) {
                final int val = i;
                Future<Boolean> fut = executor.submit(() -> {
                    boolean didAdd;
                    try {
                        didAdd = kls.add(val);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return didAdd;
                });
                wasAdded.set(val, fut);
                if (val % 1000 == 0) {
                    // do a duplicate add
                    Future<Boolean> duplicateFut = executor.submit(() -> {
                        boolean didAdd;
                        try {
                            didAdd = kls.add(val);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return didAdd;
                    });
                    duplicatesWasAdded.add(duplicateFut);
                }
            }
            int originalAdds = 0;
            int duplicateAdds = 0;
            for (Future<Boolean> fut : wasAdded) {
                if (fut.get()) {
                    originalAdds++;
                }
            }
            for (Future<Boolean> duplicateFut : duplicatesWasAdded) {
                if (duplicateFut.get()) {
                    duplicateAdds++;
                }
            }
            for (int i = 0; i < amountToAdd; i++) {
                assertTrue(kls.contains(i));
            }
            assertEquals(amountToAdd, originalAdds + duplicateAdds);
            assertEquals(amountToAdd, kls.getSize());
            assertEquals(amountToAdd / 1000, kls.getCollisions());
            executor.shutdown();
        }
    }

    public void testRemoveNoCollisions() throws Exception {
        long memCap = 100L * BYTES_IN_MB;
        int numToAdd = 195000;
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.NONE, memCap);
        // there should be no collisions for sequential positive numbers up to modulo
        for (int i = 0; i < numToAdd; i++) {
            kls.add(i);
        }
        for (int i = 0; i < 1000; i++) {
            assertTrue(kls.remove(i));
            assertFalse(kls.contains(i));
            assertFalse(kls.valueHasHadCollision(i));
        }
        assertEquals(numToAdd - 1000, kls.getSize());
    }

    public void testRemoveWithCollisions() throws Exception {
        int modulo = (int) Math.pow(2, 26);
        long memCap = 100L * BYTES_IN_MB;
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_SIX, memCap);
        for (int i = 0; i < 10; i++) {
            kls.add(i);
            if (i % 2 == 1) {
                kls.add(-i);
                assertFalse(kls.valueHasHadCollision(i));
                kls.add(i + modulo);
                assertTrue(kls.valueHasHadCollision(i));
            } else {
                assertFalse(kls.valueHasHadCollision(i));
            }
        }
        assertEquals(15, kls.getSize());
        for (int i = 0; i < 10; i++) {
            boolean didRemove = kls.remove(i);
            if (i % 2 == 1) {
                // we expect a collision with i + modulo, so we can't remove
                assertFalse(didRemove);
                assertTrue(kls.contains(i));
                // but we should be able to remove -i
                boolean didRemoveNegative = kls.remove(-i);
                assertTrue(didRemoveNegative);
                assertFalse(kls.contains(-i));
            } else {
                // we expect no collision
                assertTrue(didRemove);
                assertFalse(kls.contains(i));
                assertFalse(kls.valueHasHadCollision(i));
            }
        }
        assertEquals(5, kls.getSize());
        int offset = 12;
        kls.add(offset);
        for (int j = 1; j < 5; j++) {
            kls.add(offset + j * modulo);
        }
        assertEquals(6, kls.getSize());
        assertFalse(kls.remove(offset + modulo));
        assertTrue(kls.valueHasHadCollision(offset + 15 * modulo));
        assertTrue(kls.contains(offset + 17 * modulo));
    }

    public void testNullInputs() throws Exception {
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_NINE, 0L);
        assertFalse(kls.add(null));
        assertFalse(kls.contains(null));
        assertEquals(0, (int) kls.getInternalRepresentation(null));
        assertFalse(kls.remove(null));
        assertFalse(kls.isCollision(null, null));
        assertEquals(0, kls.getAddAttempts());
        Integer[] newVals = new Integer[] { 1, 17, -2, null, -4, null };
        kls.regenerateStore(newVals);
        assertEquals(4, kls.getSize());
    }

    public void testRemovalLogic() throws Exception {
        RBMIntKeyLookupStore.KeystoreModuloValue moduloValue = RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_SIX;
        int modulo = moduloValue.getValue();
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(moduloValue, 0L);

        // Test standard sequence: add K1, K2, K3 which all transform to C, then:
        // Remove K3
        // Remove K2, re-add it, re-remove it twice (duplicate should do nothing)
        // Remove K1, which should finally actually remove everything
        int c = -42;
        int k1 = c + modulo;
        int k2 = c + 2 * modulo;
        int k3 = c + 3 * modulo;
        kls.add(k1);
        assertTrue(kls.contains(k1));
        assertTrue(kls.contains(k3));
        kls.add(k2);
        CounterMetric numCollisions = kls.getNumCollisionsForValue(k2);
        assertNotNull(numCollisions);
        assertEquals(2, numCollisions.count());
        kls.add(k3);
        assertEquals(3, numCollisions.count());
        assertEquals(1, kls.getSize());

        boolean removed = kls.remove(k3);
        assertFalse(removed);
        HashSet<Integer> removalSet = kls.getRemovalSetForValue(k3);
        assertEquals(1, removalSet.size());
        assertTrue(removalSet.contains(k3));
        assertEquals(2, numCollisions.count());
        assertEquals(1, kls.getSize());

        removed = kls.remove(k2);
        assertFalse(removed);
        assertEquals(2, removalSet.size());
        assertTrue(removalSet.contains(k2));
        assertEquals(1, numCollisions.count());
        assertEquals(1, kls.getSize());

        kls.add(k2);
        assertEquals(1, removalSet.size());
        assertFalse(removalSet.contains(k2));
        assertEquals(2, numCollisions.count());
        assertEquals(1, kls.getSize());

        removed = kls.remove(k2);
        assertFalse(removed);
        assertEquals(2, removalSet.size());
        assertTrue(removalSet.contains(k2));
        assertEquals(1, numCollisions.count());
        assertEquals(1, kls.getSize());

        removed = kls.remove(k2);
        assertFalse(removed);
        assertEquals(2, removalSet.size());
        assertTrue(removalSet.contains(k2));
        assertEquals(1, numCollisions.count());
        assertEquals(1, kls.getSize());

        removed = kls.remove(k1);
        assertTrue(removed);
        assertNull(kls.getRemovalSetForValue(k1));
        assertNull(kls.getNumCollisionsForValue(k1));
        assertEquals(0, kls.getSize());
    }

    public void testRemovalLogicWithHashCollision() throws Exception {
        RBMIntKeyLookupStore.KeystoreModuloValue moduloValue = RBMIntKeyLookupStore.KeystoreModuloValue.TWO_TO_TWENTY_SIX;
        int modulo = moduloValue.getValue();
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(moduloValue, 0L);

        // Test adding K1 twice (maybe two keys hash to K1), then removing it twice.
        // We expect it to be unable to remove the last one, but there should be no false negatives.
        int c = 77;
        int k1 = c + modulo;
        int k2 = c + 2 * modulo;
        kls.add(k1);
        kls.add(k2);
        CounterMetric numCollisions = kls.getNumCollisionsForValue(k1);
        assertEquals(2, numCollisions.count());
        kls.add(k1);
        assertEquals(3, numCollisions.count());

        boolean removed = kls.remove(k1);
        assertFalse(removed);
        HashSet<Integer> removalSet = kls.getRemovalSetForValue(k1);
        assertTrue(removalSet.contains(k1));
        assertEquals(2, numCollisions.count());

        removed = kls.remove(k2);
        assertFalse(removed);
        assertTrue(removalSet.contains(k2));
        assertEquals(1, numCollisions.count());

        removed = kls.remove(k1);
        assertFalse(removed);
        assertTrue(removalSet.contains(k1));
        assertEquals(1, numCollisions.count());
        assertTrue(kls.contains(k1));
        assertTrue(kls.contains(k2));
    }

    public void testSetMemSizeCap() throws Exception {
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(0L); // no memory cap
        Random rand = Randomness.get();
        for (int i = 0; i < RBMIntKeyLookupStore.REFRESH_SIZE_EST_INTERVAL * 3; i++) {
            kls.add(rand.nextInt());
        }
        long memSize = kls.getMemorySizeInBytes();
        assertEquals(0, kls.getMemorySizeCapInBytes());
        kls.setMemSizeCap(new ByteSizeValue(memSize / 2, ByteSizeUnit.BYTES));
        // check the keystore is now full and has its lower cap
        assertTrue(kls.isFull());
        assertEquals(memSize / 2, kls.getMemorySizeCapInBytes());
        assertFalse(kls.add(rand.nextInt()));
    }
}
