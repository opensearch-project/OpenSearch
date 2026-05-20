/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Converts between DFA format-version representations and Lucene {@link Version}.
 *
 * <p>DFA encodes per-file format versions as a {@code long} for compactness and forward
 * compatibility with non-Lucene formats. The encoding packs each component into a base
 * {@link #VERSION_SHIFT} slot:
 * {@code major * MAJOR_SHIFT + minor * MINOR_SHIFT + bugfix}.
 * <ul>
 *   <li>9.10.0  → {@code 9_010_000}</li>
 *   <li>10.0.0  → {@code 10_000_000}</li>
 *   <li>0       → unknown / pre-versioning ({@link Version#LATEST} on decode)</li>
 * </ul>
 *
 * <p>Falls back to {@link Version#LATEST} for null/empty/unparseable inputs.
 */
@ExperimentalApi
public final class LuceneVersionConverter {

    /** Base for each version slot. Three decimal digits per component (major/minor/bugfix, each 0-999). */
    private static final long VERSION_SHIFT = 1_000L;
    /** Multiplier for the minor slot (positions 3-5). */
    private static final long MINOR_SHIFT = VERSION_SHIFT;
    /** Multiplier for the major slot (positions 6+). */
    private static final long MAJOR_SHIFT = VERSION_SHIFT * VERSION_SHIFT;

    private LuceneVersionConverter() {}

    /**
     * Parses a format-version string as a Lucene {@link Version}, falling back to
     * {@link Version#LATEST} for null, empty, or unparseable values.
     */
    public static Version toLuceneOrLatest(String formatVersion) {
        if (formatVersion == null || formatVersion.isEmpty()) {
            return Version.LATEST;
        }
        try {
            return Version.parse(formatVersion);
        } catch (Exception e) {
            return Version.LATEST;
        }
    }

    /**
     * Decodes a long-encoded format-version (see class doc) into a Lucene {@link Version}.
     * Returns {@link Version#LATEST} for {@code 0} or any value that doesn't decode to a
     * valid Lucene version.
     */
    public static Version toLuceneOrLatest(long encodedFormatVersion) {
        if (encodedFormatVersion <= 0) {
            return Version.LATEST;
        }
        int major = (int) (encodedFormatVersion / MAJOR_SHIFT);
        int minor = (int) ((encodedFormatVersion / MINOR_SHIFT) % VERSION_SHIFT);
        int bugfix = (int) (encodedFormatVersion % VERSION_SHIFT);
        try {
            return Version.fromBits(major, minor, bugfix);
        } catch (Exception e) {
            return Version.LATEST;
        }
    }

    /**
     * Encodes a Lucene {@link Version} as a long per the class-level scheme.
     * Returns {@code 0} when {@code v} is null.
     */
    public static long encode(Version v) {
        if (v == null) {
            return 0L;
        }
        return v.major * MAJOR_SHIFT + v.minor * MINOR_SHIFT + v.bugfix;
    }
}
