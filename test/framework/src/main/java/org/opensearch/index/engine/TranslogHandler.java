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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.RootObjectMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogRecoveryRunner;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.mapper.MapperRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class TranslogHandler implements TranslogRecoveryRunner {

    private final MapperService mapperService;

    private final AtomicLong appliedOperations = new AtomicLong();

    private final Engine engine;

    long appliedOperations() {
        return appliedOperations.get();
    }

    public TranslogHandler(NamedXContentRegistry xContentRegistry, IndexSettings indexSettings, Engine engine) {
        this.engine = engine;
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(AnalysisRegistry.DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()));
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, emptyMap(), emptyMap());
        SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
        MapperRegistry mapperRegistry = new IndicesModule(emptyList()).getMapperRegistry();
        mapperService = new MapperService(
            indexSettings,
            indexAnalyzers,
            xContentRegistry,
            similarityService,
            mapperRegistry,
            () -> null,
            () -> false,
            null
        );
    }

    private DocumentMapperForType docMapper(String type) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder(type);
        DocumentMapper.Builder b = new DocumentMapper.Builder(rootBuilder, mapperService);
        return new DocumentMapperForType(b.build(mapperService), null);
    }

    private void applyOperation(Engine engine, Engine.Operation operation) throws IOException {
        switch (operation.operationType()) {
            case INDEX:
                engine.index((Engine.Index) operation);
                break;
            case DELETE:
                engine.delete((Engine.Delete) operation);
                break;
            case NO_OP:
                engine.noOp((Engine.NoOp) operation);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

    @Override
    public int run(Translog.Snapshot snapshot) throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            applyOperation(engine, convertToEngineOp(operation, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY));
            opsRecovered++;
            appliedOperations.incrementAndGet();
        }
        engine.translogManager().syncTranslog();
        return opsRecovered;
    }

    public Engine.Operation convertToEngineOp(Translog.Operation operation, Engine.Operation.Origin origin) {
        // If a translog op is replayed on the primary (eg. ccr), we need to use external instead of null for its version type.
        final VersionType versionType = (origin == Engine.Operation.Origin.PRIMARY) ? VersionType.EXTERNAL : null;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                final String indexName = mapperService.index().getName();
                final Engine.Index engineIndex = engine.prepareIndex(
                    docMapper(MapperService.SINGLE_MAPPING_NAME),
                    new SourceToParse(
                        indexName,
                        index.id(),
                        index.source(),
                        MediaTypeRegistry.xContentType(index.source()),
                        index.routing()
                    ),
                    index.seqNo(),
                    index.primaryTerm(),
                    index.version(),
                    versionType,
                    origin,
                    index.getAutoGeneratedIdTimestamp(),
                    true,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                );
                return engineIndex;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                return engine.prepareDelete(
                    delete.id(),
                    delete.seqNo(),
                    delete.primaryTerm(),
                    delete.version(),
                    versionType,
                    origin,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                );
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                final Engine.NoOp engineNoOp = new Engine.NoOp(noOp.seqNo(), noOp.primaryTerm(), origin, System.nanoTime(), noOp.reason());
                return engineNoOp;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

}
