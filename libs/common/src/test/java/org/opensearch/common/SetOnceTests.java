/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common;

import org.opensearch.common.SetOnce.AlreadySetException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Random;

import static org.hamcrest.CoreMatchers.containsString;

public class SetOnceTests extends OpenSearchTestCase {
    private static final class SetOnceThread extends Thread {
        SetOnce<Integer> set;
        boolean success = false;
        final Random RAND;

        public SetOnceThread(Random random) {
            RAND = new Random(random.nextLong());
        }

        @Override
        public void run() {
            try {
                sleep(RAND.nextInt(10)); // sleep for a short time
                set.set(Integer.valueOf(getName().substring(2)));
                success = true;
            } catch (@SuppressWarnings("unused") InterruptedException e) {
                // ignore
            } catch (@SuppressWarnings("unused") RuntimeException e) {
                // TODO: change exception type
                // expected.
                success = false;
            }
        }
    }

    public void testEmptyCtor() {
        SetOnce<Integer> set = new SetOnce<>();
        assertNull(set.get());
    }

    public void testSettingCtor() {
        SetOnce<Integer> set = new SetOnce<>(5);
        assertEquals(5, set.get().intValue());

        AlreadySetException alreadySetException = expectThrows(AlreadySetException.class, () -> set.set(7));
        assertThat(alreadySetException.getMessage(), containsString("The object cannot be set twice!"));
    }

    public void testSetOnce() {
        SetOnce<Integer> set = new SetOnce<>();
        set.set(5);
        assertEquals(5, set.get().intValue());

        AlreadySetException alreadySetException = expectThrows(AlreadySetException.class, () -> set.set(7));
        assertThat(alreadySetException.getMessage(), containsString("The object cannot be set twice!"));
    }

    public void testTrySet() {
        SetOnce<Integer> set = new SetOnce<>();
        assertTrue(set.trySet(5));
        assertEquals(5, set.get().intValue());
        assertFalse(set.trySet(7));
        assertEquals(5, set.get().intValue());
    }

    public void testSetMultiThreaded() throws Exception {
        final SetOnce<Integer> set = new SetOnce<>();
        SetOnceThread[] threads = new SetOnceThread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new SetOnceThread(random());
            threads[i].setName("t-" + (i + 1));
            threads[i].set = set;
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (SetOnceThread t : threads) {
            if (t.success) {
                int expectedVal = Integer.parseInt(t.getName().substring(2));
                assertEquals("thread " + t.getName(), expectedVal, t.set.get().intValue());
            }
        }
    }
}
