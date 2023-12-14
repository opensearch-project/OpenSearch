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

public class EqualTests extends OpenSearchTestCase {

    public void testEquality() {
        Equal equalExpr = new Equal();
        Version rangeVersion = Version.fromString("1.2.3");
        assertTrue(equalExpr.evaluate(rangeVersion, Version.fromString("1.2.3")));
        assertFalse(equalExpr.evaluate(rangeVersion, Version.fromString("1.2.4")));
    }
}
