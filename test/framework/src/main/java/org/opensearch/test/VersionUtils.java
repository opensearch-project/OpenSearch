/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.test;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities for selecting versions in tests
 */
public class VersionUtils {
    // version 1.0 is removed; this is used purely to retain consistent logic for migrations
    @Deprecated
    public static Version V_1_0_0 = Version.fromId(1000099 ^ Version.MASK);

    /**
     * Sort versions that have backwards compatibility guarantees from
     * those that don't. Doesn't actually check whether or not the versions
     * are released, instead it relies on gradle to have already checked
     * this which it does in {@code :core:verifyVersions}. So long as the
     * rules here match up with the rules in gradle then this should
     * produce sensible results.
     *
     * @return a tuple containing versions with backwards compatibility
     * guarantees in v1 and versions without the guarantees in v2
     */
    static Tuple<List<Version>, List<Version>> resolveReleasedVersions(Version current, Class<?> versionClass) {
        // group versions into major version
        Map<Integer, List<Version>> majorVersions = Version.getDeclaredVersions(versionClass)
            .stream()
            .collect(Collectors.groupingBy(v -> (int) v.major));
        // this breaks b/c 5.x is still in version list but cluster-manager doesn't care about it!
        // assert majorVersions.size() == 2;
        List<List<Version>> oldVersions = new ArrayList<>(0);
        List<List<Version>> previousMajor = new ArrayList<>(0);
        if (current.major == 2) {
            // add legacy first
            oldVersions.addAll(splitByMinor(majorVersions.getOrDefault(6, Collections.emptyList())));
            previousMajor.addAll(splitByMinor(majorVersions.getOrDefault(7, Collections.emptyList())));
        }
        // TODO: remove oldVersions, we should only ever have 2 majors in Version
        // rebasing OpenSearch to 1.0.0 means the previous major version was Legacy 7.0.0
        int previousMajorID = current.major == 1 ? 7 : current.major - 1;
        oldVersions.addAll(splitByMinor(majorVersions.getOrDefault(previousMajorID - 1, Collections.emptyList())));
        previousMajor.addAll(splitByMinor(majorVersions.getOrDefault(previousMajorID, Collections.emptyList())));

        List<List<Version>> currentMajor = splitByMinor(majorVersions.get((int) current.major));

        List<Version> unreleasedVersions = new ArrayList<>();
        final List<List<Version>> stableVersions;
        if (currentMajor.size() == 1) {
            // on main branch
            stableVersions = previousMajor;
            // remove current
            moveLastToUnreleased(currentMajor, unreleasedVersions);
        } else if (current.major != 1) {
            // on a stable or release branch, ie N.x
            stableVersions = currentMajor;
            // remove the next maintenance bugfix
            final Version prevMajorLastMinor = moveLastToUnreleased(previousMajor, unreleasedVersions);
            if (prevMajorLastMinor.revision == 0 && previousMajor.isEmpty() == false) {
                // The latest minor in the previous major is a ".0" release, so there must be an unreleased bugfix for the minor before that
                moveLastToUnreleased(previousMajor, unreleasedVersions);
            }
        } else {
            stableVersions = currentMajor;
        }

        // remove last minor unless it's the first OpenSearch version.
        // all Legacy ES versions are released, so we don't exclude any.
        if (current.equals(V_1_0_0) == false) {
            // if the last minor line is Legacy there are no more staged releases; do nothing
            // otherwise the last minor line is (by definition) staged and unreleased
            Version lastMinor = moveLastToUnreleased(stableVersions, unreleasedVersions);
            // no more staged legacy bugfixes so skip;
            if (lastMinor.revision == 0) {
                // this is not a legacy version; remove the staged bugfix
                if (stableVersions.get(stableVersions.size() - 1).size() == 1) {
                    // a minor is being staged, which is also unreleased
                    moveLastToUnreleased(stableVersions, unreleasedVersions);
                }
                // remove the next bugfix
                if (stableVersions.isEmpty() == false) {
                    moveLastToUnreleased(stableVersions, unreleasedVersions);
                }
            }
        }

        // If none of the previous major was released, then the last minor and bugfix of the old version was not released either.
        if (previousMajor.isEmpty()) {
            assert currentMajor.isEmpty() : currentMajor;
            // minor of the old version is being staged
            moveLastToUnreleased(oldVersions, unreleasedVersions);
            // bugix of the old version is also being staged
            moveLastToUnreleased(oldVersions, unreleasedVersions);
        }
        List<Version> releasedVersions = Stream.of(oldVersions, previousMajor, currentMajor)
            .flatMap(List::stream)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        Collections.sort(unreleasedVersions); // we add unreleased out of order, so need to sort here
        return new Tuple<>(Collections.unmodifiableList(releasedVersions), Collections.unmodifiableList(unreleasedVersions));
    }

    // split the given versions into sub lists grouped by minor version
    private static List<List<Version>> splitByMinor(List<Version> versions) {
        Map<Integer, List<Version>> byMinor = versions.stream().collect(Collectors.groupingBy(v -> (int) v.minor));
        return byMinor.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList());
    }

    // move the last version of the last minor in versions to the unreleased versions
    private static Version moveLastToUnreleased(List<List<Version>> versions, List<Version> unreleasedVersions) {
        List<Version> lastMinor = new ArrayList<>(versions.get(versions.size() - 1));
        Version lastVersion = lastMinor.remove(lastMinor.size() - 1);
        if (lastMinor.isEmpty()) {
            versions.remove(versions.size() - 1);
        } else {
            versions.set(versions.size() - 1, lastMinor);
        }
        unreleasedVersions.add(lastVersion);
        return lastVersion;
    }

    private static final List<Version> RELEASED_VERSIONS;
    private static final List<Version> UNRELEASED_VERSIONS;
    private static final List<Version> ALL_VERSIONS;

    static {
        Tuple<List<Version>, List<Version>> versions = resolveReleasedVersions(Version.CURRENT, Version.class);
        RELEASED_VERSIONS = versions.v1();
        UNRELEASED_VERSIONS = versions.v2();
        List<Version> allVersions = new ArrayList<>(RELEASED_VERSIONS.size() + UNRELEASED_VERSIONS.size());
        allVersions.addAll(RELEASED_VERSIONS);
        allVersions.addAll(UNRELEASED_VERSIONS);
        Collections.sort(allVersions);
        ALL_VERSIONS = Collections.unmodifiableList(allVersions);
    }

    /**
     * Returns an immutable, sorted list containing all released versions.
     */
    public static List<Version> allReleasedVersions() {
        return RELEASED_VERSIONS;
    }

    /**
     * Returns an immutable, sorted list containing all unreleased versions.
     */
    public static List<Version> allUnreleasedVersions() {
        return UNRELEASED_VERSIONS;
    }

    /**
     * Returns an immutable, sorted list containing all versions, both released and unreleased.
     */
    public static List<Version> allVersions() {
        return ALL_VERSIONS;
    }

    /**
     * Returns an immutable, sorted list containing all opensearch versions; released and unreleased
     */
    public static List<Version> allOpenSearchVersions() {
        return allVersions();
    }

    /**
     * Get the version before {@code version}.
     */
    public static Version getPreviousVersion(Version version) {
        for (int i = ALL_VERSIONS.size() - 1; i >= 0; i--) {
            Version v = ALL_VERSIONS.get(i);
            if (v.before(version)) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
    }

    /**
     * Get the version before {@link Version#CURRENT}.
     */
    public static Version getPreviousVersion() {
        Version version = getPreviousVersion(Version.CURRENT);
        assert version.before(Version.CURRENT);
        return version;
    }

    /**
     * Returns the released {@link Version} before the {@link Version#CURRENT}
     * where the minor version is less than the currents minor version.
     */
    public static Version getPreviousMinorVersion() {
        for (int i = RELEASED_VERSIONS.size() - 1; i >= 0; i--) {
            Version v = RELEASED_VERSIONS.get(i);
            if (v.minor < Version.CURRENT.minor || v.major < Version.CURRENT.major) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any released versions of the minor before [" + Version.CURRENT + "]");
    }

    /**
     * Returns the oldest released {@link Version}
     */
    public static Version getFirstVersion() {
        return RELEASED_VERSIONS.get(0);
    }

    public static Version getFirstVersionOfMajor(List<Version> versions, int major) {
        Map<Integer, List<Version>> majorVersions = versions.stream().collect(Collectors.groupingBy(v -> (int) v.major));
        return majorVersions.get(major).get(0);
    }

    /**
     * Returns a random {@link Version} from all available versions.
     */
    public static Version randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
    }

    /**
     * Return a random {@link Version} from all available opensearch versions.
     **/
    public static Version randomOpenSearchVersion(Random random) {
        return randomVersion(random);
    }

    /**
     * Returns the first released (e.g., patch version 0) {@link Version} of the last minor from the requested major version
     * e.g., for version 1.0.0 this would be legacy version (7.10.0); the first release (patch 0), of the last
     * minor (for 7.x that is minor version 10) for the desired major version (7)
     **/
    public static Version lastFirstReleasedMinorFromMajor(List<Version> allVersions, int major) {
        Map<Integer, List<Version>> majorVersions = allVersions.stream().collect(Collectors.groupingBy(v -> (int) v.major));
        Map<Integer, List<Version>> groupedByMinor = majorVersions.get(major).stream().collect(Collectors.groupingBy(v -> (int) v.minor));
        List<Version> candidates = Collections.max(groupedByMinor.entrySet(), Comparator.comparing(Map.Entry::getKey)).getValue();
        return candidates.get(0);
    }

    /**
     * Returns a random {@link Version} from all available versions, that is compatible with the given version.
     */
    public static Version randomCompatibleVersion(Random random, Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream().filter(version::isCompatible).collect(Collectors.toList());
        return compatible.get(random.nextInt(compatible.size()));
    }

    /**
     * Returns a random {@link Version} between <code>minVersion</code> and <code>maxVersion</code> (inclusive).
     */
    public static Version randomVersionBetween(Random random, @Nullable Version minVersion, @Nullable Version maxVersion) {
        int minVersionIndex = 0;
        if (minVersion != null) {
            minVersionIndex = ALL_VERSIONS.indexOf(minVersion);
        }
        int maxVersionIndex = ALL_VERSIONS.size() - 1;
        if (maxVersion != null) {
            maxVersionIndex = ALL_VERSIONS.indexOf(maxVersion);
        }
        if (minVersionIndex == -1) {
            throw new IllegalArgumentException("minVersion [" + minVersion + "] does not exist.");
        } else if (maxVersionIndex == -1) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] does not exist.");
        } else if (minVersionIndex > maxVersionIndex) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] cannot be less than minVersion [" + minVersion + "]");
        } else {
            // minVersionIndex is inclusive so need to add 1 to this index
            int range = maxVersionIndex + 1 - minVersionIndex;
            return ALL_VERSIONS.get(minVersionIndex + random.nextInt(range));
        }
    }

    /**
     * returns the first future incompatible version
     */
    public static Version incompatibleFutureVersion(Version version) {
        final Optional<Version> opt = ALL_VERSIONS.stream().filter(version::before).filter(v -> v.isCompatible(version) == false).findAny();
        assert opt.isPresent() : "no future incompatible version for " + version;
        return opt.get();
    }

    /**
     * returns the first future compatible version
     */
    public static Version compatibleFutureVersion(Version version) {
        final Optional<Version> opt = ALL_VERSIONS.stream().filter(version::before).filter(v -> v.isCompatible(version)).findAny();
        assert opt.isPresent() : "no future compatible version for " + version;
        return opt.get();
    }

    /**
     * Returns the maximum {@link Version} that is compatible with the given version.
     */
    public static Version maxCompatibleVersion(Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream()
            .filter(version::isCompatible)
            .filter(version::onOrBefore)
            .collect(Collectors.toList());
        assert compatible.size() > 0;
        return compatible.get(compatible.size() - 1);
    }

    /**
     * Returns a random version index compatible with the current version.
     */
    public static Version randomIndexCompatibleVersion(Random random) {
        return randomVersionBetween(random, Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT);
    }

    /**
     * Returns a random version index compatible with the given version, but not the given version.
     */
    public static Version randomPreviousCompatibleVersion(Random random, Version version) {
        // TODO: change this to minimumCompatibilityVersion(), but first need to remove released/unreleased
        // versions so getPreviousVerison returns the *actual* previous version. Otherwise eg 8.0.0 returns say 7.0.2 for previous,
        // but 7.2.0 for minimum compat
        return randomVersionBetween(random, version.minimumIndexCompatibilityVersion(), getPreviousVersion(version));
    }

    /**
     * Returns a {@link Version} with a given major, minor and revision version.
     * Build version is skipped for the sake of simplicity.
     */
    public static Version getVersion(byte major, byte minor, byte revision) {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(revision);
        return Version.fromString(sb.toString());
    }
}
