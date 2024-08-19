/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

/**
 * This class contains constants used in the Composite Index implementation.
 */
public class CompositeIndexConstants {

    /**
     * The magic marker value used for sanity checks in the Composite Index implementation.
     */
    public static final long COMPOSITE_FIELD_MARKER = 0xC0950513F1E1DL; // Composite Field

    /**
     * Represents the key to fetch number of documents in a segment.
     */
    public static final String SEGMENT_DOCS_COUNT = "segmentDocsCount";

}
