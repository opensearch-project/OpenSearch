/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.opensearch.telemetry.tracing.Level;
import org.opensearch.test.OpenSearchTestCase;

public class LevelTests extends OpenSearchTestCase {

    public void testFromStringWithValidStrings() {
        for (Level level : Level.values()) {
            assertEquals(level, Level.fromString(level.name()));
        }
    }

    public void testFromStringWithInValidString() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> Level.fromString("randomString"));
        assertEquals(
            "invalid value for tracing level [randomString], must be in [DISABLED, ROOT, TERSE, INFO, DEBUG, TRACE]",
            exception.getMessage()
        );
    }
}
