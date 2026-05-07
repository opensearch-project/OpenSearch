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
     * Selects the immediate prior released version to compare against for API compatibility.
     * Returns null if no prior version exists on the same major line (e.g. initial major release),
     * signaling that the check should be skipped.
     */
    public static String latestPriorReleasedVersion(String currentVersion, List<String> releasedVersions) {
        Version current = Version.fromString(currentVersion);
        List<Version> candidates = releasedVersions.stream()
            .filter(version -> version.matches("\\d+\\.\\d+\\.\\d+"))
            .map(Version::fromString)
            .filter(v -> v.getMajor() == current.getMajor())
            .filter(v -> v.before(current))
            .sorted(Comparator.naturalOrder())
            .collect(Collectors.toList());

        if (candidates.isEmpty()) {
            return null;
        }

        return candidates.get(candidates.size() - 1).toString();
    }
}
