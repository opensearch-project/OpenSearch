/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.util.TreeMap;

public class FuzzyFilterPostingsFormatTests extends BasePostingsFormatTestCase {

    private TreeMap<String, FuzzySetParameters> params = new TreeMap<>() {
        @Override
        public FuzzySetParameters get(Object k) {
            return new FuzzySetParameters(() -> 0.2047);
        }
    };

    private Codec fuzzyFilterCodec = TestUtil.alwaysPostingsFormat(
        new FuzzyFilterPostingsFormat(TestUtil.getDefaultPostingsFormat(), new FuzzySetFactory(params))
    );

    @Override
    protected Codec getCodec() {
        return fuzzyFilterCodec;
    }
}
