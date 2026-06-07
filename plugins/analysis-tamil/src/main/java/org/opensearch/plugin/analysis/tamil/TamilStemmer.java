/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;

/**
 * A token filter that performs suffix stripping on Tamil tokens.
 * <p>
 * This implements a conservative, ordered (longest-first) suffix stripping algorithm
 * with a minimum stem length guard to prevent over-stemming short words.
 * <p>
 * The suffix rules are derived from Tamil morphological analysis of common inflectional
 * patterns including case markers, plural suffixes, and postpositions. This is not a
 * full morphological analyzer - it is a deterministic suffix stripper that trades
 * precision for simplicity.
 */
public final class TamilStemmer extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final int minStemLength;

    /**
     * Tamil suffixes ordered longest-first for greedy matching.
     * <p>
     * These suffixes represent common Tamil inflectional patterns:
     * - Case suffixes: க்கு (dative), இல் (locative), இன் (genitive/ablative), ஆல் (instrumental)
     * - Plural + case combinations: களுக்கு, களில், களின், களால், களைப்
     * - Plural marker: கள்
     * - Accusative: ஐ, ஐப்
     * - Sociative/comitative: ஓடு, உடன்
     * - Other postpositions and endings
     * <p>
     * This list is intentionally conservative. It focuses on high-frequency,
     * unambiguous suffixes to minimize false positives.
     */
    private static final String[] SUFFIXES = {
        // Plural + case combinations (longest first for greedy matching)
        "களுக்கு",    // plural + dative (U+0B95 U+0BB3 U+0BC1 U+0B95 U+0BCD U+0B95 U+0BC1)
        "களிடம்",     // plural + locative (at/with)
        "களால்",      // plural + instrumental
        "களின்",      // plural + genitive
        "களில்",      // plural + locative (in)
        "களை",        // plural + accusative (U+0B95 U+0BB3 U+0BC8)

        // Case/postposition suffixes
        "க்கு",       // dative (to/for) (U+0B95 U+0BCD U+0B95 U+0BC1)
        "இடம்",       // locative (at/to - standalone form)
        "உடன்",       // sociative (with)
        "வரை",        // terminative (until)
        "போல்",       // comparative (like)

        // Two-character suffixes
        "கள்",        // plural (U+0B95 U+0BB3 U+0BCD)
        "ில்",        // locative vowel-sign form (U+0BBF U+0BB2 U+0BCD) - as in வீட்டில்
        "ின்",        // genitive vowel-sign form (U+0BBF U+0BA9 U+0BCD)
        "ால்",        // instrumental vowel-sign form (U+0BBE U+0BB2 U+0BCD)
        "ோடு",        // comitative vowel-sign form (U+0BCB U+0B9F U+0BC1)
        "யை",         // accusative after vowel (U+0BAF U+0BC8) - as in குழந்தையை
        "ஆன",         // adjectival
        "ஆக",         // adverbial

        // Note: Single vowel sign suffixes like ை, ா, ே are intentionally omitted
        // because they are too aggressive and can strip meaningful parts of words.
        // The யை form (consonant + vowel sign) is safer and is included above.
    };

    /**
     * Creates a new TamilStemmer with the specified minimum stem length.
     *
     * @param input the input token stream
     * @param minStemLength the minimum length a stem must have after suffix removal;
     *                      if removing a suffix would result in a stem shorter than this,
     *                      the suffix is not removed
     */
    public TamilStemmer(TokenStream input, int minStemLength) {
        super(input);
        this.minStemLength = minStemLength;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (!input.incrementToken()) {
            return false;
        }
        stem();
        return true;
    }

    private void stem() {
        char[] buffer = termAtt.buffer();
        int length = termAtt.length();

        String term = new String(buffer, 0, length);

        for (String suffix : SUFFIXES) {
            if (term.endsWith(suffix)) {
                int newLength = term.length() - suffix.length();
                if (newLength >= minStemLength) {
                    termAtt.setLength(newLength);
                    return;
                }
            }
        }
    }
}
