/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.junit.listeners;

import org.opensearch.fips.FipsMode;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.runner.Description;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ReproduceInfoPrinterTests extends OpenSearchTestCase {

    public void testFipsReproLineSelectsTheFipsBuild() {
        final String line = reproLine(true);

        // the build derives the test JVM's java.security file and approved-only mode from this property
        assertThat(line, containsString(" -Pcrypto.standard=FIPS-140-3"));
    }

    public void testNonFipsReproLineSelectsTheDefaultBuild() {
        final String line = reproLine(false);

        assertThat(line, not(containsString("crypto.standard")));
    }

    private String reproLine(boolean fipsEnabled) {
        final FipsMode.Check original = FipsMode.CHECK;
        FipsMode.CHECK = () -> fipsEnabled;
        try {
            return ReproduceInfoPrinter.reproduceCommand(Description.createTestDescription(getClass(), getTestName()));
        } finally {
            FipsMode.CHECK = original;
        }
    }
}
