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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.uhighlight.FieldOffsetStrategy;
import org.apache.lucene.search.uhighlight.LabelledCharArrayMatcher;
import org.apache.lucene.search.uhighlight.NoOpOffsetStrategy;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.PhraseHelper;
import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.apache.lucene.search.uhighlight.UHComponents;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.opensearch.index.IndexSettings;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Subclass of the {@link UnifiedHighlighter} that works for a single field in a single document.
 * Uses a custom {@link PassageFormatter}. Accepts field content as a constructor
 * argument, given that loadings field value can be done reading from _source field.
 * Supports using different {@link BreakIterator} to break the text into fragments. Considers every distinct field
 * value as a discrete passage for highlighting (unless the whole content needs to be highlighted).
 * Supports both returning empty snippets and non highlighted snippets when no highlighting can be performed.
 */
public class CustomUnifiedHighlighter extends UnifiedHighlighter {
    public static final char MULTIVAL_SEP_CHAR = (char) 0;
    private static final Snippet[] EMPTY_SNIPPET = new Snippet[0];

    private final OffsetSource offsetSource;
    private final PassageFormatter passageFormatter;
    private final BreakIterator breakIterator;
    private final String index;
    private final String field;
    private final Locale breakIteratorLocale;
    private final int noMatchSize;
    private final CustomFieldHighlighter fieldHighlighter;
    private final int maxAnalyzedOffset;
    private final Integer fieldMaxAnalyzedOffset;

    /**
     * Creates a new instance of {@link CustomUnifiedHighlighter}
     *
     * @param analyzer the analyzer used for the field at index time, used for multi term queries internally.
     * @param offsetSource the {@link OffsetSource} to used for offsets retrieval.
     * @param passageFormatter our own {@link CustomPassageFormatter}
     *                    which generates snippets in forms of {@link Snippet} objects.
     * @param breakIteratorLocale the {@link Locale} to use for dividing text into passages.
     *                    If null {@link Locale#ROOT} is used.
     * @param breakIterator the {@link BreakIterator} to use for dividing text into passages.
     *                    If null {@link BreakIterator#getSentenceInstance(Locale)} is used.
     * @param index the index we're highlighting, mostly used for error messages
     * @param field the name of the field we're highlighting
     * @param query the query we're highlighting
     * @param noMatchSize The size of the text that should be returned when no highlighting can be performed.
     * @param maxPassages the maximum number of passes to highlight
     * @param fieldMatcher decides which terms should be highlighted
     * @param maxAnalyzedOffset if the field is more than this long we'll refuse to use the ANALYZED
     *                          offset source for it because it'd be super slow
     * @param fieldMaxAnalyzedOffset this is used to limit the length of input that will be ANALYZED, this allows bigger fields to be partially highligthed
     */
    public CustomUnifiedHighlighter(
        IndexSearcher searcher,
        Analyzer analyzer,
        OffsetSource offsetSource,
        PassageFormatter passageFormatter,
        @Nullable Locale breakIteratorLocale,
        @Nullable BreakIterator breakIterator,
        String index,
        String field,
        Query query,
        int noMatchSize,
        int maxPassages,
        Predicate<String> fieldMatcher,
        int maxAnalyzedOffset,
        Integer fieldMaxAnalyzedOffset
    ) throws IOException {
        super(searcher, analyzer);
        this.offsetSource = offsetSource;
        this.breakIterator = breakIterator;
        this.breakIteratorLocale = breakIteratorLocale == null ? Locale.ROOT : breakIteratorLocale;
        this.passageFormatter = passageFormatter;
        this.index = index;
        this.field = field;
        this.noMatchSize = noMatchSize;
        this.setFieldMatcher(fieldMatcher);
        this.maxAnalyzedOffset = maxAnalyzedOffset;
        fieldHighlighter = getFieldHighlighter(field, query, extractTerms(query), maxPassages);
        this.fieldMaxAnalyzedOffset = fieldMaxAnalyzedOffset;
    }

    /**
     * Highlights the field value.
     */
    public Snippet[] highlightField(LeafReader reader, int docId, CheckedSupplier<String, IOException> loadFieldValue) throws IOException {
        if (fieldHighlighter.getFieldOffsetStrategy() == NoOpOffsetStrategy.INSTANCE && noMatchSize == 0) {
            // If the query is such that there can't possibly be any matches then skip doing *everything*
            return EMPTY_SNIPPET;
        }
        String fieldValue = loadFieldValue.get();
        if (fieldValue == null) {
            return null;
        }
        int fieldValueLength = fieldValue.length();

        if (fieldMaxAnalyzedOffset != null && fieldMaxAnalyzedOffset > maxAnalyzedOffset) {
            throw new IllegalArgumentException(
                "max_analyzer_offset has exceeded ["
                    + maxAnalyzedOffset
                    + "] - maximum allowed to be analyzed for highlighting. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey()
                    + "] index level setting. "
                    + "For large texts, indexing with offsets or term vectors is recommended!"
            );
        }
        // if fieldMaxAnalyzedOffset is not defined
        // and if this happens we should fallback to the previous behavior
        if ((offsetSource == OffsetSource.ANALYSIS) && (fieldValueLength > maxAnalyzedOffset && fieldMaxAnalyzedOffset == null)) {
            throw new IllegalArgumentException(
                "The length of ["
                    + field
                    + "] field of ["
                    + docId
                    + "] doc of ["
                    + index
                    + "] index "
                    + "has exceeded ["
                    + maxAnalyzedOffset
                    + "] - maximum allowed to be analyzed for highlighting. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey()
                    + "] index level setting. "
                    + "For large texts, indexing with offsets or term vectors is recommended!"
            );
        }
        Snippet[] result = (Snippet[]) fieldHighlighter.highlightFieldForDoc(reader, docId, fieldValue);
        return result == null ? EMPTY_SNIPPET : result;
    }

    @Override
    protected BreakIterator getBreakIterator(String field) {
        return breakIterator;
    }

    public PassageFormatter getFormatter() {
        return passageFormatter;
    }

    @Override
    protected PassageFormatter getFormatter(String field) {
        return passageFormatter;
    }

    @Override
    protected CustomFieldHighlighter getFieldHighlighter(String field, Query query, Set<Term> allTerms, int maxPassages) {
        Predicate<String> fieldMatcher = getFieldMatcher(field);
        BytesRef[] terms = filterExtractedTerms(fieldMatcher, allTerms);
        Set<HighlightFlag> highlightFlags = getFlags(field);
        PhraseHelper phraseHelper = getPhraseHelper(field, query, highlightFlags);
        LabelledCharArrayMatcher[] automata = getAutomata(field, query, highlightFlags);
        UHComponents components = new UHComponents(field, fieldMatcher, query, terms, phraseHelper, automata, false, highlightFlags);
        OffsetSource offsetSource = getOptimizedOffsetSource(components);
        BreakIterator breakIterator = new SplittingBreakIterator(getBreakIterator(field), UnifiedHighlighter.MULTIVAL_SEP_CHAR);
        FieldOffsetStrategy strategy = getOffsetStrategy(offsetSource, components);
        return new CustomFieldHighlighter(
            field,
            strategy,
            breakIteratorLocale,
            breakIterator,
            getScorer(field),
            maxPassages,
            (noMatchSize > 0 ? 1 : 0),
            getFormatter(field),
            noMatchSize
        );
    }

    @Override
    protected Collection<Query> preSpanQueryRewrite(Query query) {
        return rewriteCustomQuery(query);
    }

    /**
     * Translate custom queries in queries that are supported by the unified highlighter.
     */
    private Collection<Query> rewriteCustomQuery(Query query) {
        if (query instanceof MultiPhrasePrefixQuery) {
            MultiPhrasePrefixQuery mpq = (MultiPhrasePrefixQuery) query;
            Term[][] terms = mpq.getTerms();
            int[] positions = mpq.getPositions();
            SpanQuery[] positionSpanQueries = new SpanQuery[positions.length];
            int sizeMinus1 = terms.length - 1;
            for (int i = 0; i < positions.length; i++) {
                SpanQuery[] innerQueries = new SpanQuery[terms[i].length];
                for (int j = 0; j < terms[i].length; j++) {
                    if (i == sizeMinus1) {
                        innerQueries[j] = new SpanMultiTermQueryWrapper<>(new PrefixQuery(terms[i][j]));
                    } else {
                        innerQueries[j] = new SpanTermQuery(terms[i][j]);
                    }
                }
                if (innerQueries.length > 1) {
                    positionSpanQueries[i] = new SpanOrQuery(innerQueries);
                } else {
                    positionSpanQueries[i] = innerQueries[0];
                }
            }

            if (positionSpanQueries.length == 1) {
                return Collections.singletonList(positionSpanQueries[0]);
            }
            // sum position increments beyond 1
            int positionGaps = 0;
            if (positions.length >= 2) {
                // positions are in increasing order. max(0,...) is just a safeguard.
                positionGaps = Math.max(0, positions[positions.length - 1] - positions[0] - positions.length + 1);
            }
            // if original slop is 0 then require inOrder
            boolean inorder = (mpq.getSlop() == 0);
            return Collections.singletonList(new SpanNearQuery(positionSpanQueries, mpq.getSlop() + positionGaps, inorder));
        } else {
            return null;
        }
    }

    /**
     * Forces the offset source for this highlighter
     */
    @Override
    protected OffsetSource getOffsetSource(String field) {
        if (offsetSource == null) {
            return super.getOffsetSource(field);
        }
        return offsetSource;
    }

    /** Customize the highlighting flags to use by field. */
    @Override
    protected Set<HighlightFlag> getFlags(String field) {
        final Set<HighlightFlag> flags = super.getFlags(field);
        // Change the defaults introduced by https://issues.apache.org/jira/browse/LUCENE-9431
        flags.remove(HighlightFlag.WEIGHT_MATCHES);
        return flags;
    }
}
