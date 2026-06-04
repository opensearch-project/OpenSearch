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
import org.apache.lucene.index.NumericDocValues;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;

/**
 * Fetches the version of a term during search phase
 *
 * @opensearch.internal
 */
public final class FetchVersionPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.version() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            NumericDocValues versions = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                versions = readerContext.reader().getNumericDocValues(VersionFieldMapper.NAME);
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                long version = Versions.NOT_FOUND;
                if (versions != null && versions.advanceExact(hitContext.docId())) {
                    version = versions.longValue();
                }
                hitContext.hit().version(version < 0 ? -1 : version);
            }
        };
    }
}
