/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.lucene90;

/**
 * This class is a custom comparator for Lucene90StoredFieldsFormat
 */
public final class Lucene90StoredFieldsFormatComparator {
    private Lucene90StoredFieldsFormatComparator() {}

    public static boolean equal(Lucene90StoredFieldsFormat one, Lucene90StoredFieldsFormat two) {
        return one.mode == two.mode;
    }
}
