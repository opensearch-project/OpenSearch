/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.test.OpenSearchTestCase;

public class StreakTests extends OpenSearchTestCase {

    public void testStreak() {
        Streak streak = new Streak();

        // Streak starts with zero.
        assertEquals(0, streak.length());

        // Streak increases on consecutive successful events.
        streak.record(true);
        assertEquals(1, streak.length());
        streak.record(true);
        assertEquals(2, streak.length());
        streak.record(true);
        assertEquals(3, streak.length());

        // Streak resets to zero after an unsuccessful event.
        streak.record(false);
        assertEquals(0, streak.length());
    }
}
