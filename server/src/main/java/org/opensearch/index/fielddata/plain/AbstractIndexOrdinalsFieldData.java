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

package org.opensearch.index.fielddata.plain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.RamAccountingTermsEnum;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.opensearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.internal.ExitableDirectoryReader;
import org.opensearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.function.Function;

/**
 * Base class for sorting ordinals field data
 *
 * @opensearch.internal
 */
public abstract class AbstractIndexOrdinalsFieldData implements IndexOrdinalsFieldData {
    private static final Logger logger = LogManager.getLogger(AbstractBinaryDVLeafFieldData.class);

    // One reload covers a load stopped for another search; a second such failure is reported, not retried.
    private static final int MAX_RELOADS = 1;

    private final String fieldName;
    private final ValuesSourceType valuesSourceType;
    private final IndexFieldDataCache cache;
    protected final CircuitBreakerService breakerService;
    protected final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;

    protected AbstractIndexOrdinalsFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        IndexFieldDataCache cache,
        CircuitBreakerService breakerService,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.cache = cache;
        this.breakerService = breakerService;
        this.scriptFunction = scriptFunction;
    }

    @Override
    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        return null;
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        if (context.reader().getFieldInfos().fieldInfo(fieldName) == null) {
            // Some leaf readers may be wrapped and report different set of fields and use the same cache key.
            // If a field can't be found then it doesn't mean it isn't there,
            // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
            // The next time the field is found, we do cache.
            return AbstractLeafOrdinalsFieldData.empty();
        }

        // A segment load reads terms through the search's own checks, which include the request timeout.
        return loadShared(
            () -> cache.<LeafOrdinalsFieldData, IndexOrdinalsFieldData>load(context, this),
            ExitableDirectoryReader.searchStopCheck(context.reader())
        );
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        IndexOrdinalsFieldData fieldData = loadGlobalInternal(indexReader);
        if (fieldData instanceof GlobalOrdinalsIndexFieldData) {
            // we create a new instance of the cached value for each consumer in order
            // to avoid creating new TermsEnums for each segment in the cached instance
            return ((GlobalOrdinalsIndexFieldData) fieldData).newConsumer(indexReader);
        } else {
            return fieldData;
        }
    }

    private IndexOrdinalsFieldData loadGlobalInternal(DirectoryReader indexReader) {
        if (indexReader.leaves().size() <= 1) {
            // ordinals are already global
            return this;
        }
        boolean fieldFound = false;
        for (LeafReaderContext context : indexReader.leaves()) {
            if (context.reader().getFieldInfos().fieldInfo(getFieldName()) != null) {
                fieldFound = true;
                break;
            }
        }
        if (fieldFound == false) {
            // Some directory readers may be wrapped and report different set of fields and use the same cache key.
            // If a field can't be found then it doesn't mean it isn't there,
            // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
            // The next time the field is found, we do cache.
            try {
                return GlobalOrdinalsBuilder.buildEmpty(indexReader, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // A global build stops only on task cancellation, so a waiting search past its timeout builds again and its
        // timeout is reported where the query phase handles it.
        return loadShared(
            () -> cache.<LeafOrdinalsFieldData, IndexOrdinalsFieldData>load(indexReader, this),
            ExitableDirectoryReader.taskCancellationCheck(indexReader)
        );
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
        return GlobalOrdinalsBuilder.build(
            ExitableDirectoryReader.taskCancellationReader(indexReader),
            this,
            breakerService,
            logger,
            scriptFunction,
            ExitableDirectoryReader.taskCancellationCheck(indexReader)
        );
    }

    /**
     * Runs a load through the shared cache, and loads once more when it failed only because another search stopped it.
     *
     * @param load the cached load
     * @param stopCheck the checks of this search that the failed load could have run
     * @return the loaded field data
     */
    private <T> T loadShared(CheckedSupplier<T, Exception> load, Runnable stopCheck) {
        Exception previous = null;
        for (int attempt = 1;; attempt++) {
            try {
                return load.get();
            } catch (Exception e) {
                if (stoppedForAnotherSearch(e, stopCheck) == false) {
                    throw asOpenSearchException(e);
                }
                if (attempt > MAX_RELOADS) {
                    final OpenSearchException failure = new OpenSearchException(
                        "[{}] fielddata load was stopped by other searches {} times",
                        e,
                        fieldName,
                        attempt
                    );
                    if (previous != null) {
                        failure.addSuppressed(previous);
                    }
                    throw failure;
                }
                logger.debug(new ParameterizedMessage("[{}] loading again after another search stopped the shared load", fieldName), e);
                previous = e;
            }
        }
    }

    /**
     * Returns whether a cached load failed only because another search stopped it, so this search can load again.
     * Concurrent searches share one cached load, and a search that waits on it gets the failure of the search that
     * ran it.
     *
     * @throws RuntimeException this search's own stop exception, with the load's failure suppressed, when this search
     *         is stopped too
     */
    private static boolean stoppedForAnotherSearch(Exception e, Runnable searchStopCheck) {
        final Throwable stop = ExceptionsHelper.unwrap(e, TaskCancelledException.class, QueryPhase.TimeExceededException.class);
        if (stop == null) {
            return false;
        }
        try {
            searchStopCheck.run();
        } catch (RuntimeException own) {
            if (own != stop) {
                own.addSuppressed(stop);
            }
            throw own;
        }
        return true;
    }

    private static OpenSearchException asOpenSearchException(Exception e) {
        return e instanceof OpenSearchException ose ? ose : new OpenSearchException(e);
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return false;
    }

    /**
     * A {@code PerValueEstimator} is a sub-class that can be used to estimate
     * the memory overhead for loading the data. Each field data
     * implementation should implement its own {@code PerValueEstimator} if it
     * intends to take advantage of the CircuitBreaker.
     * <p>
     * Note that the .beforeLoad(...) and .afterLoad(...) methods must be
     * manually called.
     *
     * @opensearch.internal
     */
    public interface PerValueEstimator {

        /**
         * @return the number of bytes for the given term
         */
        long bytesPerValue(BytesRef term);

        /**
         * Execute any pre-loading estimations for the terms. May also
         * optionally wrap a {@link TermsEnum} in a
         * {@link RamAccountingTermsEnum}
         * which will estimate the memory on a per-term basis.
         *
         * @param terms terms to be estimated
         * @return A TermsEnum for the given terms
         */
        TermsEnum beforeLoad(Terms terms) throws IOException;

        /**
         * Possibly adjust a circuit breaker after field data has been loaded,
         * now that the actual amount of memory used by the field data is known
         *
         * @param termsEnum  terms that were loaded
         * @param actualUsed actual field data memory usage
         */
        void afterLoad(TermsEnum termsEnum, long actualUsed);
    }
}
