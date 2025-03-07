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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.lucene.search.uhighlight;

import org.apache.lucene.search.uhighlight.FieldHighlighter;
import org.opensearch.search.fetch.subphase.highlight.UnifiedHighlighter;

import java.text.BreakIterator;
import java.text.CharacterIterator;
import java.util.Locale;

/**
 * A custom break iterator that is used to find break-delimited passages bounded by
 * a provided maximum length in the {@link UnifiedHighlighter} context.
 * This class uses a {@link BreakIterator} to find the last break after the provided offset
 * that would create a passage smaller than <code>maxLen</code>.
 * If the {@link BreakIterator} cannot find a passage smaller than the maximum length,
 * a secondary break iterator is used to re-split the passage at the first boundary after
 * maximum length.
 * <p>
 * This is useful to split passages created by {@link BreakIterator}s like `sentence` that
 * can create big outliers on semi-structured text.
 * <p>
 *
 * WARNING: This break iterator is designed to work with the {@link UnifiedHighlighter}.
 * <p>
 * TODO: We should be able to create passages incrementally, starting from the offset of the first match and expanding or not
 * depending on the offsets of subsequent matches. This is currently impossible because {@link FieldHighlighter} uses
 * only the first matching offset to derive the start and end of each passage.
 **/
public class BoundedBreakIteratorScanner extends BreakIterator {
    private final BreakIterator mainBreak;
    private final BreakIterator innerBreak;
    private final int maxLen;

    private int lastPrecedingOffset = -1;
    private int windowStart = -1;
    private int windowEnd = -1;
    private int innerStart = -1;
    private int innerEnd = 0;

    private BoundedBreakIteratorScanner(BreakIterator mainBreak, BreakIterator innerBreak, int maxLen) {
        this.mainBreak = mainBreak;
        this.innerBreak = innerBreak;
        this.maxLen = maxLen;
    }

    @Override
    public CharacterIterator getText() {
        return mainBreak.getText();
    }

    @Override
    public void setText(CharacterIterator newText) {
        reset();
        mainBreak.setText(newText);
        innerBreak.setText(newText);
    }

    @Override
    public void setText(String newText) {
        reset();
        mainBreak.setText(newText);
        innerBreak.setText(newText);
    }

    private void reset() {
        lastPrecedingOffset = -1;
        windowStart = -1;
        windowEnd = -1;
        innerStart = -1;
        innerEnd = 0;
    }

    /**
     * Must be called with increasing offset. See {@link FieldHighlighter} for usage.
     */
    @Override
    public int preceding(int offset) {
        if (offset < lastPrecedingOffset) {
            throw new IllegalArgumentException("offset < lastPrecedingOffset: " + "usage doesn't look like UnifiedHighlighter");
        }
        if (offset > windowStart && offset < windowEnd) {
            innerStart = innerEnd;
            innerEnd = windowEnd;
        } else {
            windowStart = innerStart = mainBreak.preceding(offset);
            windowEnd = innerEnd = mainBreak.following(offset - 1);
            // expand to next break until we reach maxLen
            while (innerEnd - innerStart < maxLen) {
                int newEnd = mainBreak.following(innerEnd);
                if (newEnd == DONE || (newEnd - innerStart) > maxLen) {
                    break;
                }
                windowEnd = innerEnd = newEnd;
            }
        }

        if (innerEnd - innerStart > maxLen) {
            // the current split is too big,
            // so starting from the current term we try to find boundaries on the left first
            if (offset - maxLen > innerStart) {
                innerStart = Math.max(innerStart, innerBreak.preceding(offset - maxLen));
            }
            // and then we try to expand the passage to the right with the remaining size
            int remaining = Math.max(0, maxLen - (offset - innerStart));
            if (offset + remaining < windowEnd) {
                innerEnd = Math.min(windowEnd, innerBreak.following(offset + remaining));
            }
        }
        lastPrecedingOffset = offset - 1;
        return innerStart;
    }

    /**
     * Can be invoked only after a call to preceding(offset+1).
     * See {@link FieldHighlighter} for usage.
     */
    @Override
    public int following(int offset) {
        if (offset != lastPrecedingOffset || innerEnd == -1) {
            throw new IllegalArgumentException("offset != lastPrecedingOffset: " + "usage doesn't look like UnifiedHighlighter");
        }
        return innerEnd;
    }

    /**
     * Returns a {@link BreakIterator#getSentenceInstance(Locale)} bounded to maxLen.
     * Secondary boundaries are found using a {@link BreakIterator#getWordInstance(Locale)}.
     */
    public static BreakIterator getSentence(Locale locale, int maxLen) {
        final BreakIterator sBreak = BreakIterator.getSentenceInstance(locale);
        final BreakIterator wBreak = BreakIterator.getWordInstance(locale);
        return new BoundedBreakIteratorScanner(sBreak, wBreak, maxLen);
    }

    @Override
    public int current() {
        // Returns the last offset of the current split
        return this.innerEnd;
    }

    @Override
    public int first() {
        throw new IllegalStateException("first() should not be called in this context");
    }

    @Override
    public int next() {
        throw new IllegalStateException("next() should not be called in this context");
    }

    @Override
    public int last() {
        throw new IllegalStateException("last() should not be called in this context");
    }

    @Override
    public int next(int n) {
        throw new IllegalStateException("next(n) should not be called in this context");
    }

    @Override
    public int previous() {
        throw new IllegalStateException("previous() should not be called in this context");
    }
}
