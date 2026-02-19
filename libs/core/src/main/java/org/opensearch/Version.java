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

package org.opensearch;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.Assertions;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * OpenSearch Version Class
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class Version implements Comparable<Version>, ToXContentFragment {
    private static final int VERSION_SHIFT = 100; // Two digits per version part
    private static final int REVISION_SHIFT = VERSION_SHIFT;
    private static final int MINOR_SHIFT = VERSION_SHIFT * VERSION_SHIFT;
    private static final int MAJOR_SHIFT = VERSION_SHIFT * VERSION_SHIFT * VERSION_SHIFT;

    /*
     * The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is alpha/beta/rc indicator AA
     * values below 25 are for alpha builder (since 5.0), and above 25 and below 50 are beta builds, and below 99 are RC builds, with 99
     * indicating a release the (internal) format of the id is there so we can easily do after/before checks on the id
     *>>
     * IMPORTANT: Unreleased vs. Released Versions
     *
     * All listed versions MUST be released versions, except the last major, the last minor and the last revision. ONLY those are required
     * as unreleased versions.
     *
     * Example: assume the last release is 2.4.0
     * The unreleased last major is the next major release, e.g. _3_.0.0
     * The unreleased last minor is the current major with a upped minor: 2._5_.0
     * The unreleased revision is the very release with a upped revision 2.4._1_
     */
    public static final int V_EMPTY_ID = 0;
    public static final Version V_EMPTY = new Version(V_EMPTY_ID, org.apache.lucene.util.Version.LATEST);

    // RELEASED
    public static final Version V_2_0_0 = new Version(2000099, org.apache.lucene.util.Version.LUCENE_9_1_0);
    public static final Version V_2_0_1 = new Version(2000199, org.apache.lucene.util.Version.LUCENE_9_1_0);
    public static final Version V_2_1_0 = new Version(2010099, org.apache.lucene.util.Version.LUCENE_9_2_0);
    public static final Version V_2_2_0 = new Version(2020099, org.apache.lucene.util.Version.LUCENE_9_3_0);
    public static final Version V_2_2_1 = new Version(2020199, org.apache.lucene.util.Version.LUCENE_9_3_0);
    public static final Version V_2_3_0 = new Version(2030099, org.apache.lucene.util.Version.LUCENE_9_3_0);
    public static final Version V_2_4_0 = new Version(2040099, org.apache.lucene.util.Version.LUCENE_9_4_1);
    public static final Version V_2_4_1 = new Version(2040199, org.apache.lucene.util.Version.LUCENE_9_4_2);
    public static final Version V_2_5_0 = new Version(2050099, org.apache.lucene.util.Version.LUCENE_9_4_2);
    public static final Version V_2_5_1 = new Version(2050199, org.apache.lucene.util.Version.LUCENE_9_4_2);

    // UNRELEASED
    public static final Version V_2_4_2 = new Version(2040299, org.apache.lucene.util.Version.LUCENE_9_4_2);
    public static final Version V_2_6_0 = new Version(2060099, org.apache.lucene.util.Version.LUCENE_9_5_0);
    public static final Version V_2_6_1 = new Version(2060199, org.apache.lucene.util.Version.LUCENE_9_5_0);
    public static final Version V_2_7_0 = new Version(2070099, org.apache.lucene.util.Version.LUCENE_9_5_0);
    public static final Version V_2_7_1 = new Version(2070199, org.apache.lucene.util.Version.LUCENE_9_5_0);
    public static final Version V_2_8_0 = new Version(2080099, org.apache.lucene.util.Version.LUCENE_9_6_0);
    public static final Version V_2_8_1 = new Version(2080199, org.apache.lucene.util.Version.LUCENE_9_6_0);
    public static final Version V_2_9_0 = new Version(2090099, org.apache.lucene.util.Version.LUCENE_9_7_0);
    public static final Version V_2_9_1 = new Version(2090199, org.apache.lucene.util.Version.LUCENE_9_7_0);
    public static final Version V_2_10_0 = new Version(2100099, org.apache.lucene.util.Version.LUCENE_9_7_0);
    public static final Version V_2_10_1 = new Version(2100199, org.apache.lucene.util.Version.LUCENE_9_7_0);
    public static final Version V_2_11_0 = new Version(2110099, org.apache.lucene.util.Version.LUCENE_9_7_0);
    public static final Version V_2_11_1 = new Version(2110199, org.apache.lucene.util.Version.LUCENE_9_7_0);
    public static final Version V_2_11_2 = new Version(2110299, org.apache.lucene.util.Version.LUCENE_9_7_0);
    public static final Version V_2_12_0 = new Version(2120099, org.apache.lucene.util.Version.LUCENE_9_9_2);
    public static final Version V_2_12_1 = new Version(2120199, org.apache.lucene.util.Version.LUCENE_9_9_2);
    public static final Version V_2_13_0 = new Version(2130099, org.apache.lucene.util.Version.LUCENE_9_10_0);
    public static final Version V_2_13_1 = new Version(2130199, org.apache.lucene.util.Version.LUCENE_9_10_0);
    public static final Version V_2_14_0 = new Version(2140099, org.apache.lucene.util.Version.LUCENE_9_10_0);
    public static final Version V_2_14_1 = new Version(2140199, org.apache.lucene.util.Version.LUCENE_9_10_0);
    public static final Version V_2_15_0 = new Version(2150099, org.apache.lucene.util.Version.LUCENE_9_10_0);
    public static final Version V_2_15_1 = new Version(2150199, org.apache.lucene.util.Version.LUCENE_9_10_0);
    public static final Version V_2_16_0 = new Version(2160099, org.apache.lucene.util.Version.LUCENE_9_11_1);
    public static final Version V_2_16_1 = new Version(2160199, org.apache.lucene.util.Version.LUCENE_9_11_1);
    public static final Version V_2_17_0 = new Version(2170099, org.apache.lucene.util.Version.LUCENE_9_11_1);
    public static final Version V_2_17_1 = new Version(2170199, org.apache.lucene.util.Version.LUCENE_9_11_1);
    public static final Version V_2_17_2 = new Version(2170299, org.apache.lucene.util.Version.LUCENE_9_11_1);
    public static final Version V_2_18_0 = new Version(2180099, org.apache.lucene.util.Version.LUCENE_9_12_0);
    public static final Version V_2_18_1 = new Version(2180199, org.apache.lucene.util.Version.LUCENE_9_12_1);
    public static final Version V_2_19_0 = new Version(2190099, org.apache.lucene.util.Version.LUCENE_9_12_1);
    public static final Version V_2_19_1 = new Version(2190199, org.apache.lucene.util.Version.LUCENE_9_12_1);
    public static final Version V_2_19_2 = new Version(2190299, org.apache.lucene.util.Version.LUCENE_9_12_1);
    public static final Version V_2_19_3 = new Version(2190399, org.apache.lucene.util.Version.LUCENE_9_12_2);
    public static final Version V_2_19_4 = new Version(2190499, org.apache.lucene.util.Version.LUCENE_9_12_3);
    public static final Version V_2_19_5 = new Version(2190599, org.apache.lucene.util.Version.LUCENE_9_12_3);
    public static final Version V_3_0_0 = new Version(3000099, org.apache.lucene.util.Version.LUCENE_10_1_0);
    public static final Version V_3_1_0 = new Version(3010099, org.apache.lucene.util.Version.LUCENE_10_2_1);
    public static final Version V_3_2_0 = new Version(3020099, org.apache.lucene.util.Version.LUCENE_10_2_2);
    public static final Version V_3_3_0 = new Version(3030099, org.apache.lucene.util.Version.LUCENE_10_3_1);
    public static final Version V_3_3_1 = new Version(3030199, org.apache.lucene.util.Version.LUCENE_10_3_1);
    public static final Version V_3_3_2 = new Version(3030299, org.apache.lucene.util.Version.LUCENE_10_3_1);
    public static final Version V_3_4_0 = new Version(3040099, org.apache.lucene.util.Version.LUCENE_10_3_2);
    public static final Version V_3_5_0 = new Version(3050099, org.apache.lucene.util.Version.LUCENE_10_3_2);
    public static final Version V_3_6_0 = new Version(3060099, org.apache.lucene.util.Version.LUCENE_10_3_2);
    public static final Version CURRENT = V_3_6_0;

    protected static final Map<Integer, Version> idToVersion;
    protected static final Map<String, Version> stringToVersion;
    static {
        final Map<Integer, Version> builder = new HashMap<>();
        final Map<String, Version> builderByString = new HashMap<>();

        for (final Field declaredField : Version.class.getFields()) {
            if (declaredField.getType().equals(Version.class)) {
                final String fieldName = declaredField.getName();
                if (fieldName.equals("CURRENT") || fieldName.equals("V_EMPTY")) {
                    continue;
                }
                assert fieldName.matches("V_\\d+_\\d+_\\d+(_alpha[1,2]|_beta[1,2]|_rc[1,2])?") : "expected Version field ["
                    + fieldName
                    + "] to match V_\\d+_\\d+_\\d+";
                try {
                    final Version version = (Version) declaredField.get(null);
                    if (Assertions.ENABLED) {
                        final String[] fields = fieldName.split("_");
                        if (fields.length == 5) {
                            assert (fields[1].equals("1") || fields[1].equals("6")) && fields[2].equals("0") : "field "
                                + fieldName
                                + " should not have a build qualifier";
                        } else {
                            final int major = Integer.valueOf(fields[1]) * MAJOR_SHIFT;
                            final int minor = Integer.valueOf(fields[2]) * MINOR_SHIFT;
                            final int revision = Integer.valueOf(fields[3]) * REVISION_SHIFT;
                            final int expectedId;
                            if (major > 0 && major < 6000000) {
                                expectedId = 0x08000000 ^ (major + minor + revision + 99);
                            } else {
                                expectedId = (major + minor + revision + 99);
                            }
                            assert version.id == expectedId : "expected version ["
                                + fieldName
                                + "] to have id ["
                                + expectedId
                                + "] but was ["
                                + version.id
                                + "]";
                        }
                    }
                    final Version maybePrevious = builder.put(version.id, version);
                    builderByString.put(version.toString(), version);
                    assert maybePrevious == null : "expected ["
                        + version.id
                        + "] to be uniquely mapped but saw ["
                        + maybePrevious
                        + "] and ["
                        + version
                        + "]";
                } catch (final IllegalAccessException e) {
                    assert false : "Version field [" + fieldName + "] should be public";
                } catch (final RuntimeException e) {
                    assert false : "Version field [" + fieldName + "] threw [" + e + "] during initialization";
                }
            }
        }
        assert CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) : "Version must be upgraded to ["
            + org.apache.lucene.util.Version.LATEST
            + "] is still set to ["
            + CURRENT.luceneVersion
            + "]";

        builder.put(V_EMPTY_ID, V_EMPTY);
        builderByString.put(V_EMPTY.toString(), V_EMPTY);
        idToVersion = Map.copyOf(builder);
        stringToVersion = Map.copyOf(builderByString);
    }

    public static Version fromId(int id) {
        if (id != 0 && (id & MASK) == 0) {
            throw new IllegalArgumentException("Version id " + id + " must contain OpenSearch mask");
        }
        final Version known = idToVersion.get(id);
        if (known != null) {
            return known;
        }
        return fromIdSlow(id);
    }

    private static Version fromIdSlow(int id) {
        // We need at least the major of the Lucene version to be correct.
        // Our best guess is to use the same Lucene version as the previous
        // version in the list, assuming that it didn't change. This is at
        // least correct for patch versions of known minors since we never
        // update the Lucene dependency for patch versions.
        List<Version> versions = DeclaredVersionsHolder.DECLARED_VERSIONS;
        Version tmp = new Version(id ^ MASK, org.apache.lucene.util.Version.LATEST);
        int index = Collections.binarySearch(versions, tmp);
        if (index < 0) {
            index = -2 - index;
        } else {
            assert false : "Version [" + tmp + "] is declared but absent from the switch statement in Version#fromId";
        }
        final org.apache.lucene.util.Version luceneVersion;
        if (index == -1) {
            // this version is older than any supported version, so we
            // assume it is the previous major to the oldest Lucene version
            // that we know about
            luceneVersion = org.apache.lucene.util.Version.fromBits(versions.get(0).luceneVersion.major - 1, 0, 0);
        } else {
            luceneVersion = versions.get(index).luceneVersion;
        }
        return new Version(id ^ MASK, luceneVersion);
    }

    public static int computeID(int major, int minor, int revision, int build) {
        return (major * MAJOR_SHIFT + minor * MINOR_SHIFT + revision * REVISION_SHIFT + build) ^ MASK;
    }

    /**
     * Returns the minimum version between the 2.
     */
    public static Version min(Version version1, Version version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version between the 2
     */
    public static Version max(Version version1, Version version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static Version fromString(String version) {
        if (stringHasLength(version) == false) { // TODO replace with Strings.hasLength after refactoring Strings to core lib
            return Version.CURRENT;
        }
        final Version cached = stringToVersion.get(version);
        if (cached != null) {
            return cached;
        }
        return fromStringSlow(version);
    }

    private static Version fromStringSlow(String version) {
        if (version.endsWith("-SNAPSHOT")) {
            throw new IllegalArgumentException("illegal version format - snapshot labels are not supported");
        }
        String[] parts = version.split("[.-]");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException(
                "the version needs to contain major, minor, and revision, and optionally the build: " + version
            );
        }

        try {
            final int rawMajor = Integer.parseInt(parts[0]);
            final int betaOffset = 25; // 0 - 24 is taking by alpha builds

            // we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            final int major = rawMajor * MAJOR_SHIFT;
            final int minor = Integer.parseInt(parts[1]) * MINOR_SHIFT;
            final int revision = Integer.parseInt(parts[2]) * REVISION_SHIFT;
            if (major > 99000000 || minor > 990000 || revision > 9900) {
                throw new IllegalArgumentException("Version parts must be <= 99");
            }
            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("alpha")) {
                    build = Integer.parseInt(buildStr.substring(5));
                    assert build < 25 : "expected a alpha build but " + build + " >= 25";
                } else if (buildStr.startsWith("Beta") || buildStr.startsWith("beta")) {
                    build = betaOffset + Integer.parseInt(buildStr.substring(4));
                    assert build < 50 : "expected a beta build but " + build + " >= 50";
                } else if (buildStr.startsWith("RC") || buildStr.startsWith("rc")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                } else {
                    throw new IllegalArgumentException("unable to parse version " + version);
                }
            }

            return fromId((major + minor + revision + build) ^ MASK);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    public static final int MASK = 0x08000000;
    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    public final org.apache.lucene.util.Version luceneVersion;

    Version(int id, org.apache.lucene.util.Version luceneVersion) {
        // flip the 28th bit of the ID; identify as an opensearch vs legacy system:
        // we start from version 1 for opensearch, so ignore the 0 (empty) version
        if (id != 0) {
            this.id = id ^ MASK;
            id &= 0xF7FFFFFF;
        } else {
            this.id = id;
        }
        this.major = (byte) ((id / MAJOR_SHIFT) % VERSION_SHIFT);
        this.minor = (byte) ((id / MINOR_SHIFT) % VERSION_SHIFT);
        this.revision = (byte) ((id / REVISION_SHIFT) % VERSION_SHIFT);
        this.build = (byte) (id % VERSION_SHIFT);
        this.luceneVersion = Objects.requireNonNull(luceneVersion);
        this.minCompatVersion = null;
        this.minIndexCompatVersion = null;
    }

    public boolean after(Version version) {
        return version.id < id;
    }

    public boolean onOrAfter(Version version) {
        return version.id <= id;
    }

    public boolean before(Version version) {
        return version.id > id;
    }

    public boolean onOrBefore(Version version) {
        return version.id >= id;
    }

    public int compareMajor(Version other) {
        // comparing Legacy 7x for bwc
        // todo: remove the following when removing legacy support in 3.0.0
        if (major == 7 || other.major == 7 || major == 6 || other.major == 6) {
            // opensearch v1.x and v2.x need major translation to compare w/ legacy versions
            int m = major == 1 ? 7 : major == 2 ? 8 : major;
            int om = other.major == 1 ? 7 : other.major == 2 ? 8 : other.major;
            return Integer.compare(m, om);
        }
        return Integer.compare(major, other.major);
    }

    @Override
    public int compareTo(Version other) {
        return Integer.compare(this.id, other.id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }

    /*
     * We need the declared versions when computing the minimum compatibility version. As computing the declared versions uses reflection it
     * is not cheap. Since computing the minimum compatibility version can occur often, we use this holder to compute the declared versions
     * lazily once.
     */
    static class DeclaredVersionsHolder {
        protected static final List<Version> DECLARED_VERSIONS = Collections.unmodifiableList(getDeclaredVersions(Version.class));
    }

    // lazy initialized because we don't yet have the declared versions ready when instantiating the cached Version
    // instances
    protected Version minCompatVersion = null;

    // lazy initialized because we don't yet have the declared versions ready when instantiating the cached Version
    // instances
    protected Version minIndexCompatVersion = null;

    /**
     * Returns the minimum compatible version based on the current
     * version. Ie a node needs to have at least the return version in order
     * to communicate with a node running the current version. The returned version
     * is in most of the cases the smallest major version release unless the current version
     * is a beta or RC release then the version itself is returned.
     */
    public Version minimumCompatibilityVersion() {
        Version res = minCompatVersion;
        if (res == null) {
            res = computeMinCompatVersion();
            minCompatVersion = res;
        }
        return res;
    }

    protected Version computeMinCompatVersion() {
        if (major >= 3) {
            // all major versions from 3 onwards are compatible with last minor series of the previous major
            Version bwcVersion = null;

            for (int i = DeclaredVersionsHolder.DECLARED_VERSIONS.size() - 1; i >= 0; i--) {
                final Version candidateVersion = DeclaredVersionsHolder.DECLARED_VERSIONS.get(i);
                if (candidateVersion.major == major - 1 && candidateVersion.isRelease() && after(candidateVersion)) {
                    if (bwcVersion != null && candidateVersion.minor < bwcVersion.minor) {
                        break;
                    }
                    bwcVersion = candidateVersion;
                }
            }
            return bwcVersion == null ? this : bwcVersion;
        }

        return Version.min(this, fromId(maskId(1000099)));
    }

    /**
     * this is used to ensure the version id for new versions of OpenSearch are always less than the predecessor versions
     */
    protected int maskId(final int id) {
        return MASK ^ id;
    }

    /**
     * Returns the minimum created index version that this version supports. Indices created with lower versions
     * can't be used with this version. This should also be used for file based serialization backwards compatibility ie. on serialization
     * code that is used to read / write file formats like transaction logs, cluster state, and index metadata.
     */
    public Version minimumIndexCompatibilityVersion() {
        Version res = minIndexCompatVersion;
        if (res == null) {
            res = computeMinIndexCompatVersion();
            minIndexCompatVersion = res;
        }
        return res;
    }

    protected Version computeMinIndexCompatVersion() {
        final int bwcMajor = major - 1;
        final int bwcMinor = 0;
        return Version.min(this, fromId((bwcMajor * MAJOR_SHIFT + bwcMinor * MINOR_SHIFT + 99) ^ MASK));
    }

    /**
     * Returns <code>true</code> iff both version are compatible. Otherwise <code>false</code>
     */
    public boolean isCompatible(Version version) {
        boolean compatible = onOrAfter(version.minimumCompatibilityVersion()) && version.onOrAfter(minimumCompatibilityVersion());

        // OpenSearch version 1 is the functional equivalent of predecessor version 7
        // OpenSearch version 2 is the functional equivalent of predecessor version 8
        // todo refactor this logic after removing deprecated features
        int a = major;
        int b = version.major;

        if (a == 7 || b == 7 || a == 6 || b == 6) {
            if (major <= 2) {
                a += 6; // for legacy compatibility up to version 2.x (to compare minCompat)
            }
            if (version.major <= 2) {
                b += 6; // for legacy compatibility up to version 2.x (to compare minCompat)
            }
        }

        assert compatible == false || Math.max(a, b) - Math.min(a, b) <= 1;
        return compatible;
    }

    @SuppressForbidden(reason = "System.out.*")
    public static void main(String[] args) {
        final String versionOutput = String.format(
            Locale.ROOT,
            "Version: %s, Build: %s/%s/%s/%s, JVM: %s",
            Build.CURRENT.getQualifiedVersion(),
            Build.CURRENT.type().displayName(),
            Build.CURRENT.hash(),
            Build.CURRENT.date(),
            System.getProperty("java.version")  // TODO switch back to JvmInfo.jvmInfo().version() after refactoring to core lib
        );
        System.out.println(versionOutput);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(revision);
        if (isAlpha()) {
            sb.append("-alpha");
            sb.append(build);
        } else if (isBeta()) {
            sb.append("-beta");
            sb.append(build - 25);
        } else if (build < 99) {
            sb.append("-rc");
            sb.append(build - 50);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version = (Version) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public boolean isBeta() {
        return build >= 25 && build < 50;
    }

    /**
     * Returns true iff this version is an alpha version
     * Note: This has been introduced in version 5 of the OpenSearch predecessor. Previous versions will never
     * have an alpha version.
     */
    public boolean isAlpha() {
        return build < 25;
    }

    public boolean isRC() {
        return build > 50 && build < 99;
    }

    public boolean isRelease() {
        return build == 99;
    }

    /**
     * Extracts a sorted list of declared version constants from a class.
     * The argument would normally be Version.class but is exposed for
     * testing with other classes-containing-version-constants.
     */
    public static List<Version> getDeclaredVersions(final Class<?> versionClass) {
        final Field[] fields = versionClass.getFields();
        final List<Version> versions = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final int mod = field.getModifiers();
            if (false == Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                continue;
            }
            if (field.getType() != Version.class) {
                continue;
            }
            switch (field.getName()) {
                case "CURRENT":
                case "V_EMPTY":
                    continue;
            }
            assert field.getName().matches("V(_\\d+)+(_(alpha|beta|rc)\\d+)?") : field.getName();
            try {
                versions.add(((Version) field.get(null)));
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        Collections.sort(versions);
        return versions;
    }

    /**
     * Check that the given String is neither <code>null</code> nor of length 0.
     * Note: Will return <code>true</code> for a String that purely consists of whitespace.
     */
    public static boolean stringHasLength(String str) {
        return (str != null && str.length() > 0);
    }
}
