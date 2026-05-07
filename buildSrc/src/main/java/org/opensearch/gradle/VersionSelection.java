/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class VersionSelection {

    private VersionSelection() {}

    /**
     * Selects the latest released OpenSearch version before the current minor line.
     * Maintenance branches must not compare against same-minor or newer-minor releases.
     */
    public static String latestReleasedBeforeCurrentMinor(String currentVersion, List<String> releasedVersions) {
        Version current = Version.fromString(currentVersion);
        List<Version> candidates = releasedVersions.stream()
            .filter(version -> version.matches("\\d+\\.\\d+\\.\\d+"))
            .map(Version::fromString)
            .filter(releasedVersion -> isBeforeCurrentMinor(current, releasedVersion))
            .sorted(Comparator.naturalOrder())
            .collect(Collectors.toList());

        if (candidates.isEmpty()) {
            throw new IllegalStateException("Unable to find a released version before " + current + "'s minor line");
        }

        return candidates.get(candidates.size() - 1).toString();
    }

    private static boolean isBeforeCurrentMinor(Version current, Version releasedVersion) {
        if (releasedVersion.getMajor() < current.getMajor()) {
            return true;
        }
        return releasedVersion.getMajor() == current.getMajor() && releasedVersion.getMinor() < current.getMinor();
    }
}
