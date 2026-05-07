/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.Test;

import java.util.List;

public class VersionSelectionTests extends GradleUnitTestCase {

    @Test
    public void selectsLatestReleasedVersionBeforeCurrentMinor() {
        assertEquals("3.4.0", VersionSelection.latestReleasedBeforeCurrentMinor("3.5.1", List.of("3.4.0", "3.5.0", "3.6.0")));
    }

    @Test
    public void selectsLatestPatchBeforeCurrentMinor() {
        assertEquals("3.6.1", VersionSelection.latestReleasedBeforeCurrentMinor("3.7.0", List.of("3.5.0", "3.6.0", "3.6.1")));
    }

    @Test
    public void selectsLatestReleaseFromPreviousMajorForFirstMinor() {
        assertEquals("3.6.1", VersionSelection.latestReleasedBeforeCurrentMinor("4.0.0", List.of("3.6.0", "3.6.1", "4.0.0")));
    }

    @Test
    public void ignoresQualifiedReleaseCandidates() {
        assertEquals(
            "2.19.5",
            VersionSelection.latestReleasedBeforeCurrentMinor(
                "3.0.0-beta1",
                List.of("2.19.4", "2.19.5", "3.0.0-alpha1", "3.0.0-beta1", "3.0.0")
            )
        );
    }
}
