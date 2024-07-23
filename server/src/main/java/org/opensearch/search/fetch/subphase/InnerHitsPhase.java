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

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Gets the inner hits of a document during search
 *
 * @opensearch.internal
 */
public final class InnerHitsPhase implements FetchSubPhase {

    private final FetchPhase fetchPhase;

    public InnerHitsPhase(FetchPhase fetchPhase) {
        this.fetchPhase = fetchPhase;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext searchContext) {
        if (searchContext.innerHits() == null) {
            return null;
        }
        Map<String, InnerHitsContext.InnerHitSubContext> innerHits = searchContext.innerHits().getInnerHits();
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                SourceLookup rootLookup = searchContext.getRootSourceLookup(hitContext);
                hitExecute(innerHits, hit, rootLookup);
            }
        };
    }

    private void hitExecute(Map<String, InnerHitsContext.InnerHitSubContext> innerHits, SearchHit hit, SourceLookup rootLookup)
        throws IOException {
        for (Map.Entry<String, InnerHitsContext.InnerHitSubContext> entry : innerHits.entrySet()) {
            InnerHitsContext.InnerHitSubContext innerHitsContext = entry.getValue();
            TopDocsAndMaxScore topDoc = innerHitsContext.topDocs(hit);

            Map<String, SearchHits> results = hit.getInnerHits();
            if (results == null) {
                hit.setInnerHits(results = new HashMap<>());
            }
            innerHitsContext.queryResult().topDocs(topDoc, innerHitsContext.sort() == null ? null : innerHitsContext.sort().formats);
            int[] docIdsToLoad = new int[topDoc.topDocs.scoreDocs.length];
            for (int j = 0; j < topDoc.topDocs.scoreDocs.length; j++) {
                docIdsToLoad[j] = topDoc.topDocs.scoreDocs[j].doc;
            }
            innerHitsContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
            innerHitsContext.setId(hit.getId());
            innerHitsContext.setRootLookup(rootLookup);

            fetchPhase.execute(innerHitsContext);
            FetchSearchResult fetchResult = innerHitsContext.fetchResult();
            SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
            for (int j = 0; j < internalHits.length; j++) {
                ScoreDoc scoreDoc = topDoc.topDocs.scoreDocs[j];
                SearchHit searchHitFields = internalHits[j];
                searchHitFields.score(scoreDoc.score);
                if (scoreDoc instanceof FieldDoc) {
                    FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                    searchHitFields.sortValues(fieldDoc.fields, innerHitsContext.sort().formats);
                }
            }
            results.put(entry.getKey(), fetchResult.hits());
        }
    }
}
