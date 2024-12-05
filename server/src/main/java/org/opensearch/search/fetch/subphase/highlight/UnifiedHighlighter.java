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

package org.opensearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.BoundedBreakIteratorScanner;
import org.apache.lucene.search.uhighlight.CustomPassageFormatter;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.Snippet;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter.OffsetSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.text.Text;
import org.opensearch.index.mapper.DerivedFieldType;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

/**
 * Uses lucene's unified highlighter implementation
 *
 * @opensearch.internal
 */
public class UnifiedHighlighter implements Highlighter {
    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, CustomUnifiedHighlighter> cache = (Map<String, CustomUnifiedHighlighter>) fieldContext.cache.computeIfAbsent(
            UnifiedHighlighter.class.getName(),
            k -> new HashMap<>()
        );
        if (cache.containsKey(fieldContext.fieldName) == false) {
            cache.put(fieldContext.fieldName, buildHighlighter(fieldContext));
        }
        CustomUnifiedHighlighter highlighter = cache.get(fieldContext.fieldName);
        MappedFieldType fieldType = fieldContext.fieldType;
        SearchHighlightContext.Field field = fieldContext.field;
        FetchSubPhase.HitContext hitContext = fieldContext.hitContext;

        CheckedSupplier<String, IOException> loadFieldValues = () -> {
            List<Object> fieldValues = loadFieldValues(
                highlighter,
                fieldContext.context.getQueryShardContext(),
                fieldType,
                hitContext,
                fieldContext.forceSource
            );
            if (fieldValues.size() == 0) {
                return null;
            }
            return mergeFieldValues(fieldValues, MULTIVAL_SEP_CHAR);
        };
        Snippet[] fieldSnippets = highlighter.highlightField(hitContext.reader(), hitContext.docId(), loadFieldValues);

        if (fieldSnippets == null || fieldSnippets.length == 0) {
            return null;
        }
        List<Snippet> snippets = new ArrayList<>(fieldSnippets.length);
        for (Snippet fieldSnippet : fieldSnippets) {
            if (Strings.hasText(fieldSnippet.getText())) {
                snippets.add(fieldSnippet);
            }
        }
        if (snippets.isEmpty()) {
            return null;
        }

        if (field.fieldOptions().scoreOrdered()) {
            // let's sort the snippets by score if needed
            CollectionUtil.introSort(snippets, (o1, o2) -> Double.compare(o2.getScore(), o1.getScore()));
        }

        String[] fragments = new String[snippets.size()];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = snippets.get(i).getText();
        }

        return new HighlightField(fieldContext.fieldName, Text.convertFromStringArray(fragments));
    }

    public AnalyzerWrapper getLimitedOffsetAnalyzer(Analyzer analyzer, int limit) {
        return new AnalyzerWrapper(analyzer.getReuseStrategy()) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                return analyzer;
            }

            @Override
            protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
                return new TokenStreamComponents(components.getSource(), new LimitTokenOffsetFilter(components.getTokenStream(), limit));
            }

        };

    }

    CustomUnifiedHighlighter buildHighlighter(FieldHighlightContext fieldContext) throws IOException {
        Encoder encoder = fieldContext.field.fieldOptions().encoder().equals("html")
            ? HighlightUtils.Encoders.HTML
            : HighlightUtils.Encoders.DEFAULT;
        int maxAnalyzedOffset = fieldContext.context.getIndexSettings().getHighlightMaxAnalyzedOffset();
        Integer fieldMaxAnalyzedOffset = fieldContext.field.fieldOptions().maxAnalyzerOffset();
        int numberOfFragments = fieldContext.field.fieldOptions().numberOfFragments();
        Analyzer analyzer = getAnalyzer(fieldContext.context.mapperService().documentMapper());
        MappedFieldType derivedFieldType = fieldContext.context.getQueryShardContext().resolveDerivedFieldType(fieldContext.fieldName);
        if (derivedFieldType != null) {
            analyzer = ((DerivedFieldType) derivedFieldType).getIndexAnalyzer();
        }
        if (fieldMaxAnalyzedOffset != null) {
            analyzer = getLimitedOffsetAnalyzer(analyzer, fieldMaxAnalyzedOffset);
        }
        PassageFormatter passageFormatter = getPassageFormatter(fieldContext.hitContext, fieldContext.field, encoder);
        IndexSearcher searcher = fieldContext.context.searcher();
        OffsetSource offsetSource = getOffsetSource(fieldContext.fieldType);
        BreakIterator breakIterator;
        int higlighterNumberOfFragments;
        if (numberOfFragments == 0
            // non-tokenized fields should not use any break iterator (ignore boundaryScannerType)
            || fieldContext.fieldType.getTextSearchInfo().isTokenized() == false) {
            /*
             * We use a control char to separate values, which is the
             * only char that the custom break iterator breaks the text
             * on, so we don't lose the distinction between the different
             * values of a field and we get back a snippet per value
             */
            breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
            higlighterNumberOfFragments = numberOfFragments == 0 ? Integer.MAX_VALUE - 1 : numberOfFragments;
        } else {
            // using paragraph separator we make sure that each field value holds a discrete passage for highlighting
            breakIterator = getBreakIterator(fieldContext.field);
            higlighterNumberOfFragments = numberOfFragments;
        }
        return new CustomUnifiedHighlighter(
            searcher,
            analyzer,
            offsetSource,
            passageFormatter,
            fieldContext.field.fieldOptions().boundaryScannerLocale(),
            breakIterator,
            fieldContext.context.getIndexName(),
            fieldContext.fieldName,
            fieldContext.query,
            fieldContext.field.fieldOptions().noMatchSize(),
            higlighterNumberOfFragments,
            fieldMatcher(fieldContext),
            maxAnalyzedOffset,
            fieldMaxAnalyzedOffset
        );
    }

    protected PassageFormatter getPassageFormatter(HitContext hitContext, SearchHighlightContext.Field field, Encoder encoder) {
        return new CustomPassageFormatter(field.fieldOptions().preTags()[0], field.fieldOptions().postTags()[0], encoder);
    }

    protected Analyzer getAnalyzer(DocumentMapper docMapper) {
        return docMapper.mappers().indexAnalyzer();
    }

    protected List<Object> loadFieldValues(
        CustomUnifiedHighlighter highlighter,
        QueryShardContext context,
        MappedFieldType fieldType,
        FetchSubPhase.HitContext hitContext,
        boolean forceSource
    ) throws IOException {
        List<Object> fieldValues = HighlightUtils.loadFieldValues(fieldType, context, hitContext, forceSource);
        fieldValues = fieldValues.stream().map((s) -> convertFieldValue(fieldType, s)).collect(Collectors.toList());
        return fieldValues;
    }

    protected BreakIterator getBreakIterator(SearchHighlightContext.Field field) {
        final SearchHighlightContext.FieldOptions fieldOptions = field.fieldOptions();
        final Locale locale = fieldOptions.boundaryScannerLocale() != null ? fieldOptions.boundaryScannerLocale() : Locale.ROOT;
        final HighlightBuilder.BoundaryScannerType type = fieldOptions.boundaryScannerType() != null
            ? fieldOptions.boundaryScannerType()
            : HighlightBuilder.BoundaryScannerType.SENTENCE;
        int maxLen = fieldOptions.fragmentCharSize();
        switch (type) {
            case SENTENCE:
                if (maxLen > 0) {
                    return BoundedBreakIteratorScanner.getSentence(locale, maxLen);
                }
                return BreakIterator.getSentenceInstance(locale);
            case WORD:
                // ignore maxLen
                return BreakIterator.getWordInstance(locale);
            default:
                throw new IllegalArgumentException("Invalid boundary scanner type: " + type.toString());
        }
    }

    protected static String convertFieldValue(MappedFieldType type, Object value) {
        if (value instanceof BytesRef) {
            return type.valueForDisplay(value).toString();
        } else {
            return value.toString();
        }
    }

    protected static String mergeFieldValues(List<Object> fieldValues, char valuesSeparator) {
        // postings highlighter accepts all values in a single string, as offsets etc. need to match with content
        // loaded from stored fields, we merge all values using a proper separator
        String rawValue = Strings.collectionToDelimitedString(fieldValues, String.valueOf(valuesSeparator));
        return rawValue.substring(0, Math.min(rawValue.length(), Integer.MAX_VALUE - 1));
    }

    protected OffsetSource getOffsetSource(MappedFieldType fieldType) {
        TextSearchInfo tsi = fieldType.getTextSearchInfo();
        if (tsi.hasOffsets()) {
            return tsi.termVectors() != TextSearchInfo.TermVector.NONE ? OffsetSource.POSTINGS_WITH_TERM_VECTORS : OffsetSource.POSTINGS;
        }
        if (tsi.termVectors() == TextSearchInfo.TermVector.OFFSETS) {
            return OffsetSource.TERM_VECTORS;
        }
        return OffsetSource.ANALYSIS;
    }

    private Predicate<String> fieldMatcher(FieldHighlightContext fieldContext) {
        if (fieldContext.field.fieldOptions().requireFieldMatch()) {
            String fieldName = fieldContext.fieldName;
            return fieldName::equals;
        }
        // ignore terms that targets the _id field since they use a different encoding
        // that is not compatible with utf8
        return name -> IdFieldMapper.NAME.equals(name) == false;
    }
}
