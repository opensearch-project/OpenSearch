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
import org.apache.lucene.document.Field;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FragmentsBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.index.analysis.AnalyzerComponentsProvider;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Comparator;
import java.util.List;

/**
 * Simple helper class for {@link FastVectorHighlighter} {@link FragmentsBuilder} implementations.
 *
 * @opensearch.internal
 */
public final class FragmentBuilderHelper {

    private FragmentBuilderHelper() {
        // no instance
    }

    /**
     * Fixes problems with broken analysis chains if positions and offsets are messed up that can lead to
     * {@link StringIndexOutOfBoundsException} in the {@link FastVectorHighlighter}
     */
    public static WeightedFragInfo fixWeightedFragInfo(MappedFieldType fieldType, Field[] values, WeightedFragInfo fragInfo) {
        assert fragInfo != null : "FragInfo must not be null";
        assert fieldType.name().equals(values[0].name()) : "Expected MappedFieldType for field " + values[0].name();
        if (!fragInfo.getSubInfos().isEmpty() && containsBrokenAnalysis(fieldType.indexAnalyzer())) {
            /* This is a special case where broken analysis like WDF is used for term-vector creation at index-time
             * which can potentially mess up the offsets. To prevent a SAIIOBException we need to resort
             * the fragments based on their offsets rather than using solely the positions as it is done in
             * the FastVectorHighlighter. Yet, this is really a lucene problem and should be fixed in lucene rather
             * than in this hack... aka. "we are are working on in!" */
            final List<SubInfo> subInfos = fragInfo.getSubInfos();
            CollectionUtil.introSort(subInfos, new Comparator<SubInfo>() {
                @Override
                public int compare(SubInfo o1, SubInfo o2) {
                    int startOffset = o1.termsOffsets().get(0).getStartOffset();
                    int startOffset2 = o2.termsOffsets().get(0).getStartOffset();
                    return FragmentBuilderHelper.compare(startOffset, startOffset2);
                }
            });
            return new WeightedFragInfo(
                Math.min(fragInfo.getSubInfos().get(0).termsOffsets().get(0).getStartOffset(), fragInfo.getStartOffset()),
                fragInfo.getEndOffset(),
                subInfos,
                fragInfo.getTotalBoost()
            );
        } else {
            return fragInfo;
        }
    }

    private static int compare(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    private static boolean containsBrokenAnalysis(Analyzer analyzer) {
        // TODO maybe we need a getter on Namedanalyzer that tells if this uses broken Analysis
        if (analyzer instanceof NamedAnalyzer) {
            analyzer = ((NamedAnalyzer) analyzer).analyzer();
        }
        if (analyzer instanceof AnalyzerComponentsProvider) {
            final TokenFilterFactory[] tokenFilters = ((AnalyzerComponentsProvider) analyzer).getComponents().getTokenFilters();
            for (TokenFilterFactory tokenFilterFactory : tokenFilters) {
                if (tokenFilterFactory.breaksFastVectorHighlighter()) {
                    return true;
                }
            }
        }
        return false;
    }
}
