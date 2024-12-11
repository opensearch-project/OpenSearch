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

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class Version implements Comparable<Version>, ToXContentFragment {
    /*
     * The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is alpha/beta/rc indicator AA
     * values below 25 are for alpha builder (since 5.0), and above 25 and below 50 are beta builds, and below 99 are RC builds, with 99
     * indicating a release the (internal) format of the id is there so we can easily do after/before checks on the id
     *
     * IMPORTANT: Unreleased vs. Released Versions
     *
     * All listed versions MUST be released versions, except the last major, the last minor and the last revison. ONLY those are required
     * as unreleased versions.
     *
     * Example: assume the last release is 7.3.0
     * The unreleased last major is the next major release, e.g. _8_.0.0
     * The unreleased last minor is the current major with a upped minor: 7._4_.0
     * The unreleased revision is the very release with a upped revision 7.3._1_
     */
    public static final int V_EMPTY_ID = 0;
    public static final Version V_EMPTY = new Version(V_EMPTY_ID, org.apache.lucene.util.Version.LATEST);

    public static final Version V_1_0_0 = new Version(1000099, org.apache.lucene.util.Version.LUCENE_8_8_2);
    public static final Version V_1_0_1 = new Version(1000199, org.apache.lucene.util.Version.LUCENE_8_8_2);
    public static final Version V_1_1_0 = new Version(1010099, org.apache.lucene.util.Version.LUCENE_8_9_0);
    public static final Version V_1_1_1 = new Version(1010199, org.apache.lucene.util.Version.LUCENE_8_9_0);
    public static final Version V_1_2_0 = new Version(1020099, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_2_1 = new Version(1020199, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_2_2 = new Version(1020299, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_2_3 = new Version(1020399, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_2_4 = new Version(1020499, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_2_5 = new Version(1020599, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_0 = new Version(1030099, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_1 = new Version(1030199, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_2 = new Version(1030299, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_3 = new Version(1030399, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_4 = new Version(1030499, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_5 = new Version(1030599, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_6 = new Version(1030699, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_7 = new Version(1030799, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_8 = new Version(1030899, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_9 = new Version(1030999, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_10 = new Version(1031099, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_11 = new Version(1031199, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_12 = new Version(1031299, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_13 = new Version(1031399, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_14 = new Version(1031499, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_15 = new Version(1031599, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_16 = new Version(1031699, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_17 = new Version(1031799, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_18 = new Version(1031899, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_19 = new Version(1031999, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_20 = new Version(1032099, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_3_21 = new Version(1032199, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version V_1_4_0 = new Version(1040099, org.apache.lucene.util.Version.LUCENE_8_10_1);
    public static final Version CURRENT = V_1_4_0;

    public static Version readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static Version fromId(int id) {
        final Version known = LegacyESVersion.idToVersion.get(id);
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
        Version tmp = id < MASK
            ? new LegacyESVersion(id, org.apache.lucene.util.Version.LATEST)
            : new Version(id ^ MASK, org.apache.lucene.util.Version.LATEST);
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
        return id < MASK ? new LegacyESVersion(id, luceneVersion) : new Version(id ^ MASK, luceneVersion);
    }

    /**
     * Return the {@link Version} of OpenSearch that has been used to create an index given its settings.
     *
     * @throws IllegalStateException if the given index settings doesn't contain a value for the key
     *         {@value IndexMetadata#SETTING_VERSION_CREATED}
     */
    public static Version indexCreated(Settings indexSettings) {
        final Version indexVersion = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        if (indexVersion.equals(V_EMPTY)) {
            final String message = String.format(
                Locale.ROOT,
                "[%s] is not present in the index settings for index with UUID [%s]",
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                indexSettings.get(IndexMetadata.SETTING_INDEX_UUID)
            );
            throw new IllegalStateException(message);
        }
        return indexVersion;
    }

    public static void writeVersion(Version version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    public static int computeLegacyID(int major, int minor, int revision, int build) {
        return major * 1000000 + minor * 10000 + revision * 100 + build;
    }

    public static int computeID(int major, int minor, int revision, int build) {
        return computeLegacyID(major, minor, revision, build) ^ MASK;
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
        if (!Strings.hasLength(version)) {
            return Version.CURRENT;
        }
        final Version cached = LegacyESVersion.stringToVersion.get(version);
        if (cached != null) {
            return cached;
        }
        {
            // get major string; remove when creating OpenSearch 3.0
            String[] parts = version.split("[.-]");
            if (parts.length < 3 || parts.length > 4) {
                throw new IllegalArgumentException(
                    "the version needs to contain major, minor, and revision, and optionally the build: " + version
                );
            }
            int major = Integer.parseInt(parts[0]);
            if (major > 3) {
                return LegacyESVersion.fromStringSlow(version);
            }
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
            final int major = rawMajor * 1000000;
            final int minor = Integer.parseInt(parts[1]) * 10000;
            final int revision = Integer.parseInt(parts[2]) * 100;
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
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
        this.luceneVersion = Objects.requireNonNull(luceneVersion);
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

    // LegacyESVersion major 7 is equivalent to Version major 1
    public int compareMajor(Version other) {
        int m = major == 1 ? 7 : major == 2 ? 8 : major;
        int om = other.major == 1 ? 7 : other.major == 2 ? 8 : other.major;
        return Integer.compare(m, om);
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
        // use LegacyESVersion.class since it inherits Version fields
        protected static final List<Version> DECLARED_VERSIONS = Collections.unmodifiableList(getDeclaredVersions(LegacyESVersion.class));
    }

    // lazy initialized because we don't yet have the declared versions ready when instantiating the cached Version
    // instances
    private Version minCompatVersion;

    // lazy initialized because we don't yet have the declared versions ready when instantiating the cached Version
    // instances
    private Version minIndexCompatVersion;

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
        if (major == 1) {
            return LegacyESVersion.V_6_8_0;
        } else if (major == 6) {
            // force the minimum compatibility for version 6 to 5.6 since we don't reference version 5 anymore
            return Version.fromId(5060099);
        } else if (major == 2) {
            return LegacyESVersion.V_7_10_0;
        } else if (major >= 7) {
            // all major versions from 7 onwards are compatible with last minor series of the previous major
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

        return Version.min(this, fromId(maskId((int) major * 1000000 + 0 * 10000 + 99)));
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

    private Version computeMinIndexCompatVersion() {
        final int bwcMajor;
        if (major == 5) {
            bwcMajor = 2; // we jumped from 2 to 5
        } else if (major == 7 || major == 1) {
            return LegacyESVersion.V_6_0_0_beta1;
        } else if (major == 2) {
            return LegacyESVersion.V_7_0_0;
        } else {
            bwcMajor = major - 1;
        }
        final int bwcMinor = 0;
        return Version.min(this, fromId((bwcMajor * 1000000 + bwcMinor * 10000 + 99)));
    }

    /**
     * Returns <code>true</code> iff both version are compatible. Otherwise <code>false</code>
     */
    public boolean isCompatible(Version version) {
        boolean compatible = onOrAfter(version.minimumCompatibilityVersion()) && version.onOrAfter(minimumCompatibilityVersion());

        // OpenSearch version 1 is the functional equivalent of predecessor version 7
        // OpenSearch version 2 is the functional equivalent of predecessor unreleased version "8"
        // todo refactor this logic after removing deprecated features
        int a = major;
        if (major == 1) {
            a = 7;
        } else if (major == 2) {
            a = 8;
        }
        int b = version.major;
        if (version.major == 1) {
            b = 7;
        } else if (version.major == 2) {
            b = 8;
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
            JvmInfo.jvmInfo().version()
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
            if (field.getType() != Version.class && field.getType() != LegacyESVersion.class) {
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
}
