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
    public void selectsImmediatePriorVersion() {
        assertEquals("3.5.0", VersionSelection.latestPriorReleasedVersion("3.5.1", List.of("3.4.0", "3.5.0", "3.6.0")));
    }

    @Test
    public void selectsLatestPatchOnSameMinor() {
        assertEquals("3.6.1", VersionSelection.latestPriorReleasedVersion("3.7.0", List.of("3.5.0", "3.6.0", "3.6.1")));
    }

    @Test
    public void returnsNullForInitialMajorRelease() {
        assertNull(VersionSelection.latestPriorReleasedVersion("4.0.0", List.of("3.6.0", "3.6.1")));
    }

    @Test
    public void ignoresQualifiedVersions() {
        assertEquals(
            "2.19.5",
            VersionSelection.latestPriorReleasedVersion("2.20.0", List.of("2.19.4", "2.19.5", "2.20.0-alpha1", "2.20.0-beta1"))
        );
    }

    @Test
    public void returnsNullForPreReleaseOfInitialMajor() {
        assertNull(VersionSelection.latestPriorReleasedVersion("4.0.0-beta1", List.of("3.6.0", "3.6.1")));
    }

    @Test
    public void selectsPriorVersionOnSameMajorOnly() {
        assertEquals("4.0.0", VersionSelection.latestPriorReleasedVersion("4.1.0", List.of("3.6.0", "3.6.1", "4.0.0")));
    }

    @Test
    public void handlesSnapshotVersion() {
        assertEquals("3.5.0", VersionSelection.latestPriorReleasedVersion("3.6.0-SNAPSHOT", List.of("3.4.0", "3.5.0")));
    }
}
