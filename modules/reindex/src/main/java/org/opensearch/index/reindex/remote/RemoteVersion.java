/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex.remote;

import java.util.Objects;

/**
 * Represents a version of a remote Elasticsearch or OpenSearch cluster for reindexing purposes.
 * This class provides backward compatibility support for communicating with older Elasticsearch versions
 * without relying on the global Version class.
 */
public final class RemoteVersion implements Comparable<RemoteVersion> {

    private final int major;
    private final int minor;
    private final int revision;
    final boolean isOpenSearch;

    // Common version constants for backward compatibility
    public static final RemoteVersion ELASTICSEARCH_0_20_5 = new RemoteVersion(0, 20, 5, false);
    public static final RemoteVersion ELASTICSEARCH_0_90_13 = new RemoteVersion(0, 90, 13, false);
    public static final RemoteVersion ELASTICSEARCH_1_0_0 = new RemoteVersion(1, 0, 0, false);
    public static final RemoteVersion ELASTICSEARCH_1_7_5 = new RemoteVersion(1, 7, 5, false);
    public static final RemoteVersion ELASTICSEARCH_2_0_0 = new RemoteVersion(2, 0, 0, false);
    public static final RemoteVersion ELASTICSEARCH_2_1_0 = new RemoteVersion(2, 1, 0, false);
    public static final RemoteVersion ELASTICSEARCH_2_3_3 = new RemoteVersion(2, 3, 3, false);
    public static final RemoteVersion ELASTICSEARCH_5_0_0 = new RemoteVersion(5, 0, 0, false);
    public static final RemoteVersion ELASTICSEARCH_6_0_0 = new RemoteVersion(6, 0, 0, false);
    public static final RemoteVersion ELASTICSEARCH_6_3_0 = new RemoteVersion(6, 3, 0, false);
    public static final RemoteVersion ELASTICSEARCH_7_0_0 = new RemoteVersion(7, 0, 0, false);

    // OpenSearch versions
    public static final RemoteVersion OPENSEARCH_1_0_0 = new RemoteVersion(1, 0, 0, true);
    public static final RemoteVersion OPENSEARCH_2_0_0 = new RemoteVersion(2, 0, 0, true);
    public static final RemoteVersion OPENSEARCH_3_1_0 = new RemoteVersion(3, 1, 0, true);

    public RemoteVersion(int major, int minor, int revision, boolean isOpenSearch) {
        this.major = major;
        this.minor = minor;
        this.revision = revision;
        this.isOpenSearch = isOpenSearch;
    }

    /**
     * Parse version string like "7.10.2" or "1.0.0"
     */
    public static RemoteVersion fromString(String version, boolean isOpenSearch) {
        if (version == null || version.trim().isEmpty()) {
            throw new IllegalArgumentException("Version string cannot be null or empty");
        }

        // Remove snapshot and pre-release qualifiers
        String cleanVersion = version.replace("-SNAPSHOT", "").replaceFirst("-(alpha\\d+|beta\\d+|rc\\d+)", "");

        String[] parts = cleanVersion.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid version format: " + version);
        }

        try {
            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);
            int revision = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;

            return new RemoteVersion(major, minor, revision, isOpenSearch);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid version format: " + version, e);
        }
    }

    public boolean before(RemoteVersion other) {
        return this.compareTo(other) < 0;
    }

    public boolean onOrAfter(RemoteVersion other) {
        return this.compareTo(other) >= 0;
    }

    public boolean onOrBefore(RemoteVersion other) {
        return this.compareTo(other) <= 0;
    }

    public boolean after(RemoteVersion other) {
        return this.compareTo(other) > 0;
    }

    @Override
    public int compareTo(RemoteVersion other) {
        if (other == null) {
            return 1;
        }

        if (this.isOpenSearch != other.isOpenSearch) {
            return this.isOpenSearch ? 1 : -1;
        }

        int result = Integer.compare(this.major, other.major);
        if (result != 0) {
            return result;
        }

        result = Integer.compare(this.minor, other.minor);
        if (result != 0) {
            return result;
        }

        return Integer.compare(this.revision, other.revision);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteVersion that = (RemoteVersion) obj;
        return major == that.major && minor == that.minor && revision == that.revision && Objects.equals(isOpenSearch, that.isOpenSearch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, revision, isOpenSearch);
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + revision;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getRevision() {
        return revision;
    }
}
