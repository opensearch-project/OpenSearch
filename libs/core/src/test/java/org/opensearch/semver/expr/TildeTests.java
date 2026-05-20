/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.semver.expr;

import org.opensearch.Version;
import org.opensearch.test.OpenSearchTestCase;

public class TildeTests extends OpenSearchTestCase {

    public void testPatchVersionVariability() {
        Tilde tildeExpr = new Tilde();
        Version rangeVersion = Version.fromString("1.2.3");

        assertTrue(tildeExpr.evaluate(rangeVersion, Version.fromString("1.2.3")));
        assertTrue(tildeExpr.evaluate(rangeVersion, Version.fromString("1.2.4")));
        assertTrue(tildeExpr.evaluate(rangeVersion, Version.fromString("1.2.9")));

        assertFalse(tildeExpr.evaluate(rangeVersion, Version.fromString("1.2.0")));
        assertFalse(tildeExpr.evaluate(rangeVersion, Version.fromString("1.2.2")));
        assertFalse(tildeExpr.evaluate(rangeVersion, Version.fromString("1.3.0")));
        assertFalse(tildeExpr.evaluate(rangeVersion, Version.fromString("2.0.0")));
    }
}
