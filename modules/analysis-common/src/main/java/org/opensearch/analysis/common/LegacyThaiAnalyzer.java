/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.DecimalDigitFilter;

/**
 * The {@code thai} analyzer as it was before Lucene 10.6, kept for indices created before
 * {@link org.opensearch.Version#V_3_10_0}: {@link LegacyThaiTokenizer}, lowercase, decimal digits, stop.
 *
 * <p>Lucene 10.6's {@link org.apache.lucene.analysis.th.ThaiAnalyzer} also applies {@code ThaiCharFilter},
 * {@code ThaiNormalizationFilter} and {@code ThaiRepeatFilter} and ships a different default stopword list, which changes the
 * terms produced for some text. Using it for existing indices would stop their documents from matching.
 */
final class LegacyThaiAnalyzer extends StopwordAnalyzerBase {

    LegacyThaiAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new LegacyThaiTokenizer();
        TokenStream result = new LowerCaseFilter(source);
        result = new DecimalDigitFilter(result);
        result = new StopFilter(result, stopwords);
        return new TokenStreamComponents(source, result);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        TokenStream result = new LowerCaseFilter(in);
        return new DecimalDigitFilter(result);
    }
}
