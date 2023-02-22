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

package org.opensearch.plugins;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.lucene.search.function.ScoreFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryParser;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionParser;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationParsingFunction;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTerms;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.opensearch.search.aggregations.pipeline.MovAvgModel;
import org.opensearch.search.aggregations.pipeline.MovAvgPipelineAggregator;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.search.rescore.Rescorer;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortParser;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.Suggester;
import org.opensearch.search.suggest.SuggestionBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Plugin for extending search time behavior.
 *
 * @opensearch.api
 */
public interface SearchPlugin {
    /**
     * The new {@link ScoreFunction}s defined by this plugin.
     */
    default List<ScoreFunctionSpec<?>> getScoreFunctions() {
        return emptyList();
    }

    /**
     * The new {@link SignificanceHeuristic}s defined by this plugin. {@linkplain SignificanceHeuristic}s are used by the
     * {@link SignificantTerms} aggregation to pick which terms are significant for a given query.
     */
    default List<SignificanceHeuristicSpec<?>> getSignificanceHeuristics() {
        return emptyList();
    }

    /**
     * The new {@link MovAvgModel}s defined by this plugin. {@linkplain MovAvgModel}s are used by the {@link MovAvgPipelineAggregator} to
     * model trends in data.
     */
    default List<SearchExtensionSpec<MovAvgModel, MovAvgModel.AbstractModelParser>> getMovingAverageModels() {
        return emptyList();
    }

    /**
     * The new {@link FetchSubPhase}s defined by this plugin.
     */
    default List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return emptyList();
    }

    /**
     * The new {@link SearchExtBuilder}s defined by this plugin.
     */
    default List<SearchExtSpec<?>> getSearchExts() {
        return emptyList();
    }

    /**
     * Get the {@link Highlighter}s defined by this plugin.
     */
    default Map<String, Highlighter> getHighlighters() {
        return emptyMap();
    }

    /**
     * The new {@link Suggester}s defined by this plugin.
     */
    default List<SuggesterSpec<?>> getSuggesters() {
        return emptyList();
    }

    /**
     * The new {@link Query}s defined by this plugin.
     */
    default List<QuerySpec<?>> getQueries() {
        return emptyList();
    }

    /**
     * The new {@link Sort}s defined by this plugin.
     */
    default List<SortSpec<?>> getSorts() {
        return emptyList();
    }

    /**
     * The new {@link Aggregation}s added by this plugin.
     */
    default List<AggregationSpec> getAggregations() {
        return emptyList();
    }

    /**
     * Allows plugins to register new aggregations using aggregation names that are already defined
     * in Core, as long as the new aggregations target different ValuesSourceTypes
     * @return A list of the new registrar functions
     */
    default List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return emptyList();
    }

    /**
     * Allows plugins to register new Aggregation in the {@link CompositeAggregation}.
     *
     * @return A {@link List} of {@link CompositeAggregationSpec}
     */
    default List<CompositeAggregationSpec> getCompositeAggregations() {
        return emptyList();
    }

    /**
     * The new {@link PipelineAggregator}s added by this plugin.
     */
    default List<PipelineAggregationSpec> getPipelineAggregations() {
        return emptyList();
    }

    /**
     * The new {@link Rescorer}s added by this plugin.
     */
    default List<RescorerSpec<?>> getRescorers() {
        return emptyList();
    }

    /**
     * The new {@link QueryPhaseSearcher} added by this plugin. At the moment, only one {@link QueryPhaseSearcher} is supported per
     * instance, the {@link IllegalStateException} is going to be thrown if more then one plugin tries to register
     * {@link QueryPhaseSearcher} implementation.
     */
    default Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        return Optional.empty();
    }

    /**
     * The executor service provider (registered through {@link Plugin#getExecutorBuilders(Settings)} to be used at search
     * time by {@link IndexSearcher}. The {@link IllegalStateException} is going to be thrown if more then one
     * plugin tries to register index searcher executor.
     */
    default Optional<ExecutorServiceProvider> getIndexSearcherExecutorProvider() {
        return Optional.empty();
    }

    /**
     * Executor service provider
     */
    interface ExecutorServiceProvider {
        /**
         * Provides an executor service instance
         * @param threadPool thread pool
         * @return executor service instance
         */
        ExecutorService getExecutor(ThreadPool threadPool);
    }

    /**
     * Specification of custom {@link ScoreFunction}.
     */
    class ScoreFunctionSpec<T extends ScoreFunctionBuilder<T>> extends SearchExtensionSpec<T, ScoreFunctionParser<T>> {
        /**
         * Specification of custom {@link ScoreFunctionBuilder}.
         *
         * @param name holds the names by which this score function might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the score function should use as its
         *        {@link NamedWriteable#getWriteableName()} too.  It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this score function's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the score function builder from xcontent
         */
        public ScoreFunctionSpec(ParseField name, Writeable.Reader<T> reader, ScoreFunctionParser<T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link ScoreFunctionBuilder}.
         *
         * @param name the name by which this score function might be parsed or deserialized. Make sure that the score function builder returns this name for
         *        {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param reader the reader registered for this score function's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the score function builder from xcontent
         */
        public ScoreFunctionSpec(String name, Writeable.Reader<T> reader, ScoreFunctionParser<T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification of custom {@link SignificanceHeuristic}.
     */
    class SignificanceHeuristicSpec<T extends SignificanceHeuristic> extends SearchExtensionSpec<T, BiFunction<XContentParser, Void, T>> {
        /**
         * Specification of custom {@link SignificanceHeuristic}.
         *
         * @param name holds the names by which this heuristic might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the heuristic should use as its
         *        {@link NamedWriteable#getWriteableName()} too.  It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this heuristic. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser function that reads the heuristic from xcontent
         */
        public SignificanceHeuristicSpec(ParseField name, Writeable.Reader<T> reader, BiFunction<XContentParser, Void, T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link SignificanceHeuristic}.
         *
         * @param name the name by which this heuristic might be parsed or deserialized. Make sure that the heuristic returns this name for
         *        {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param reader the reader registered for this heuristic. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser function that reads the heuristic from xcontent
         */
        public SignificanceHeuristicSpec(String name, Writeable.Reader<T> reader, BiFunction<XContentParser, Void, T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification for a {@link Suggester}.
     */
    class SuggesterSpec<T extends SuggestionBuilder<T>> extends SearchExtensionSpec<T, CheckedFunction<XContentParser, T, IOException>> {

        private Writeable.Reader<? extends Suggest.Suggestion> suggestionReader;

        /**
         * Specification of custom {@link Suggester}.
         *
         * @param name holds the names by which this suggester might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the request builder and Suggestion response readers are registered. So it is the name that the
         *        query and Suggestion response should use as their {@link NamedWriteable#getWriteableName()} return values too.  It is
         *        an error if {@link ParseField#getPreferredName()} conflicts with another registered name, including names from other plugins.
         * @param builderReader the reader registered for this suggester's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param builderParser a parser that reads the suggester's builder from xcontent
         * @param suggestionReader the reader registered for this suggester's Suggestion response. Typically, a reference to a constructor
         *        that takes a {@link StreamInput}
         */
        public SuggesterSpec(
            ParseField name,
            Writeable.Reader<T> builderReader,
            CheckedFunction<XContentParser, T, IOException> builderParser,
            Writeable.Reader<? extends Suggest.Suggestion> suggestionReader
        ) {

            super(name, builderReader, builderParser);
            setSuggestionReader(suggestionReader);
        }

        /**
         * Specification of custom {@link Suggester}.
         *
         * @param name the name by which this suggester might be parsed or deserialized. Make sure that the query builder and Suggestion
         *        response reader return this name for {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts
         *        with another registered name, including names from other plugins.
         * @param builderReader the reader registered for this suggester's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param builderParser a parser that reads the suggester's builder from xcontent
         * @param suggestionReader the reader registered for this suggester's Suggestion response. Typically, a reference to a constructor
         *        that takes a {@link StreamInput}
         */
        public SuggesterSpec(
            String name,
            Writeable.Reader<T> builderReader,
            CheckedFunction<XContentParser, T, IOException> builderParser,
            Writeable.Reader<? extends Suggest.Suggestion> suggestionReader
        ) {

            super(name, builderReader, builderParser);
            setSuggestionReader(suggestionReader);
        }

        private void setSuggestionReader(Writeable.Reader<? extends Suggest.Suggestion> reader) {
            this.suggestionReader = reader;
        }

        /**
         * Returns the reader used to read the {@link Suggest.Suggestion} generated by this suggester
         */
        public Writeable.Reader<? extends Suggest.Suggestion> getSuggestionReader() {
            return this.suggestionReader;
        }
    }

    /**
     * Specification of custom {@link Query}.
     */
    class QuerySpec<T extends QueryBuilder> extends SearchExtensionSpec<T, QueryParser<T>> {
        /**
         * Specification of custom {@link Query}.
         *
         * @param name holds the names by which this query might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the query should use as its
         *        {@link NamedWriteable#getWriteableName()} too.  It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this query's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the query builder from xcontent
         */
        public QuerySpec(ParseField name, Writeable.Reader<T> reader, QueryParser<T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link Query}.
         *
         * @param name the name by which this query might be parsed or deserialized. Make sure that the query builder returns this name for
         *        {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param reader the reader registered for this query's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the query builder from xcontent
         */
        public QuerySpec(String name, Writeable.Reader<T> reader, QueryParser<T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification of custom {@link Sort}.
     */
    class SortSpec<T extends SortBuilder<T>> extends SearchExtensionSpec<T, SortParser<T>> {
        /**
         * Specification of custom {@link Sort}.
         *
         * @param name holds the names by which this sort might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the sort should use as its
         *        {@link NamedWriteable#getWriteableName()} too.  It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this sort's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the sort builder from xcontent
         */
        public SortSpec(ParseField name, Writeable.Reader<T> reader, SortParser<T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link Sort}.
         *
         * @param name the name by which this sort might be parsed or deserialized. Make sure that the sort builder returns this name for
         *        {@link NamedWriteable#getWriteableName()}. It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param reader the reader registered for this sort's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the sort builder from xcontent
         */
        public SortSpec(String name, Writeable.Reader<T> reader, SortParser<T> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification for an {@link Aggregation}.
     */
    class AggregationSpec extends SearchExtensionSpec<AggregationBuilder, ContextParser<String, ? extends AggregationBuilder>> {
        private final Map<String, Writeable.Reader<? extends InternalAggregation>> resultReaders = new TreeMap<>();
        private Consumer<ValuesSourceRegistry.Builder> aggregatorRegistrar;

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the {@link AggregationBuilder} should return
         *        from {@link NamedWriteable#getWriteableName()}. It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         */
        public <T extends AggregationBuilder> AggregationSpec(
            ParseField name,
            Writeable.Reader<T> reader,
            ContextParser<String, T> parser
        ) {
            super(name, reader, parser);
        }

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name the name by which this aggregation might be parsed or deserialized. Make sure that the {@link AggregationBuilder}
         *        returns this from {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another
         *        registered name, including names from other plugins.
         * @param reader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         */
        public <T extends AggregationBuilder> AggregationSpec(String name, Writeable.Reader<T> reader, ContextParser<String, T> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the {@link AggregationBuilder} should return
         *        from {@link NamedWriteable#getWriteableName()}. It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         * @deprecated Use the ctor that takes a {@link ContextParser} instead
         */
        @Deprecated
        public AggregationSpec(ParseField name, Writeable.Reader<? extends AggregationBuilder> reader, Aggregator.Parser parser) {
            super(name, reader, (p, aggName) -> parser.parse(aggName, p));
        }

        /**
         * Specification for an {@link Aggregation}.
         *
         * @param name the name by which this aggregation might be parsed or deserialized. Make sure that the {@link AggregationBuilder}
         *        returns this from {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another
         *        registered name, including names from other plugins.
         * @param reader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser the reads the aggregation builder from xcontent
         * @deprecated Use the ctor that takes a {@link ContextParser} instead
         */
        @Deprecated
        public AggregationSpec(String name, Writeable.Reader<? extends AggregationBuilder> reader, Aggregator.Parser parser) {
            super(name, reader, (p, aggName) -> parser.parse(aggName, p));
        }

        /**
         * Add a reader for the shard level results of the aggregation with {@linkplain #getName}'s {@link ParseField#getPreferredName()} as
         * the {@link NamedWriteable#getWriteableName()}.
         */
        public AggregationSpec addResultReader(Writeable.Reader<? extends InternalAggregation> resultReader) {
            return addResultReader(getName().getPreferredName(), resultReader);
        }

        /**
         * Add a reader for the shard level results of the aggregation.
         */
        public AggregationSpec addResultReader(String writeableName, Writeable.Reader<? extends InternalAggregation> resultReader) {
            resultReaders.put(writeableName, resultReader);
            return this;
        }

        /**
         * Get the readers that must be registered for this aggregation's results.
         */
        public Map<String, Writeable.Reader<? extends InternalAggregation>> getResultReaders() {
            return resultReaders;
        }

        /**
         * Get the function to register the {@link org.opensearch.search.aggregations.support.ValuesSource} to aggregator mappings for
         * this aggregation
         */
        public Consumer<ValuesSourceRegistry.Builder> getAggregatorRegistrar() {
            return aggregatorRegistrar;
        }

        /**
         * Set the function to register the {@link org.opensearch.search.aggregations.support.ValuesSource} to aggregator mappings for
         * this aggregation
         */
        public AggregationSpec setAggregatorRegistrar(Consumer<ValuesSourceRegistry.Builder> aggregatorRegistrar) {
            this.aggregatorRegistrar = aggregatorRegistrar;
            return this;
        }
    }

    /**
     * Specification for including an aggregation as a part of Composite Aggregation.
     */
    class CompositeAggregationSpec {
        private final Consumer<ValuesSourceRegistry.Builder> aggregatorRegistrar;
        private final Class<?> valueSourceBuilderClass;
        @Deprecated
        /** This is added for backward compatibility, you don't need to set it, as we use aggregationType instead of
         * byte code
         */
        private Byte byteCode;
        private final CompositeAggregationParsingFunction parsingFunction;
        private final String aggregationType;
        private final Writeable.Reader<? extends CompositeValuesSourceBuilder<?>> reader;

        /**
         * Specification for registering an aggregation in Composite Aggregation
         *
         * @param aggregatorRegistrar function to register the
         * {@link org.opensearch.search.aggregations.support.ValuesSource} to aggregator mappings for Composite
         *                            aggregation
         * @param valueSourceBuilderClass ValueSourceBuilder class name which is building the aggregation
         * @param byteCode byte code which is used in serialisation and de-serialisation to indentify which
         *                 aggregation builder to use
         * @param reader Typically, a reference to a constructor that takes a {@link StreamInput}, which is
         *               registered with the aggregation
         * @param parsingFunction a reference function which will be used to parse the Aggregation input.
         * @param aggregationType a {@link String} defined in the AggregationBuilder as type.
         */
        public CompositeAggregationSpec(
            final Consumer<ValuesSourceRegistry.Builder> aggregatorRegistrar,
            final Class<? extends CompositeValuesSourceBuilder<?>> valueSourceBuilderClass,
            final Byte byteCode,
            final Writeable.Reader<? extends CompositeValuesSourceBuilder<?>> reader,
            final CompositeAggregationParsingFunction parsingFunction,
            final String aggregationType
        ) {
            this.aggregatorRegistrar = aggregatorRegistrar;
            this.valueSourceBuilderClass = valueSourceBuilderClass;
            this.byteCode = byteCode;
            this.parsingFunction = parsingFunction;
            this.aggregationType = aggregationType;
            this.reader = reader;
        }

        public Consumer<ValuesSourceRegistry.Builder> getAggregatorRegistrar() {
            return aggregatorRegistrar;
        }

        public Class<?> getValueSourceBuilderClass() {
            return valueSourceBuilderClass;
        }

        public Byte getByteCode() {
            return byteCode;
        }

        public CompositeAggregationParsingFunction getParsingFunction() {
            return parsingFunction;
        }

        public String getAggregationType() {
            return aggregationType;
        }

        public Writeable.Reader<? extends CompositeValuesSourceBuilder<?>> getReader() {
            return reader;
        }
    }

    /**
     * Specification for a {@link PipelineAggregator}.
     */
    class PipelineAggregationSpec extends SearchExtensionSpec<
        PipelineAggregationBuilder,
        ContextParser<String, ? extends PipelineAggregationBuilder>> {
        private final Map<String, Writeable.Reader<? extends InternalAggregation>> resultReaders = new TreeMap<>();

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the readers are registered. So it is the name that the {@link PipelineAggregationBuilder} and
         *        {@link PipelineAggregator} should return from {@link NamedWriteable#getWriteableName()}.  It is an error if
         *        {@link ParseField#getPreferredName()} conflicts with another registered name, including names from other plugins.
         * @param builderReader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser reads the aggregation builder from XContent
         */
        public PipelineAggregationSpec(
            ParseField name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            ContextParser<String, ? extends PipelineAggregationBuilder> parser
        ) {
            super(name, builderReader, parser);
        }

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name name by which this aggregation might be parsed or deserialized. Make sure it is the name that the
         *        {@link PipelineAggregationBuilder} and {@link PipelineAggregator} should return from
         *        {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param builderReader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser reads the aggregation builder from XContent

         */
        public PipelineAggregationSpec(
            String name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            ContextParser<String, ? extends PipelineAggregationBuilder> parser
        ) {
            super(name, builderReader, parser);
        }

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name holds the names by which this aggregation might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the readers are registered. So it is the name that the {@link PipelineAggregationBuilder} and
         *        {@link PipelineAggregator} should return from {@link NamedWriteable#getWriteableName()}.  It is an error if
         *        {@link ParseField#getPreferredName()} conflicts with another registered name, including names from other plugins.
         * @param builderReader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser reads the aggregation builder from XContent
         * @deprecated prefer the ctor that takes a {@link ContextParser}
         */
        @Deprecated
        public PipelineAggregationSpec(
            ParseField name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            PipelineAggregator.Parser parser
        ) {
            super(name, builderReader, (p, n) -> parser.parse(n, p));
        }

        /**
         * Specification of a {@link PipelineAggregator}.
         *
         * @param name name by which this aggregation might be parsed or deserialized. Make sure it is the name that the
         *        {@link PipelineAggregationBuilder} and {@link PipelineAggregator} should return from
         *        {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param builderReader the reader registered for this aggregation's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @deprecated prefer the ctor that takes a {@link ContextParser}
         */
        @Deprecated
        public PipelineAggregationSpec(
            String name,
            Writeable.Reader<? extends PipelineAggregationBuilder> builderReader,
            PipelineAggregator.Parser parser
        ) {
            super(name, builderReader, (p, n) -> parser.parse(n, p));
        }

        /**
         * Add a reader for the shard level results of the aggregation with {@linkplain #getName()}'s {@link ParseField#getPreferredName()}
         * as the {@link NamedWriteable#getWriteableName()}.
         */
        public PipelineAggregationSpec addResultReader(Writeable.Reader<? extends InternalAggregation> resultReader) {
            return addResultReader(getName().getPreferredName(), resultReader);
        }

        /**
         * Add a reader for the shard level results of the aggregation.
         */
        public PipelineAggregationSpec addResultReader(String writeableName, Writeable.Reader<? extends InternalAggregation> resultReader) {
            resultReaders.put(writeableName, resultReader);
            return this;
        }

        /**
         * Get the readers that must be registered for this aggregation's results.
         */
        public Map<String, Writeable.Reader<? extends InternalAggregation>> getResultReaders() {
            return resultReaders;
        }
    }

    /**
     * Specification for a {@link SearchExtBuilder} which represents an additional section that can be
     * parsed in a search request (within the ext element).
     */
    class SearchExtSpec<T extends SearchExtBuilder> extends SearchExtensionSpec<T, CheckedFunction<XContentParser, T, IOException>> {
        /**
         * Specification of custom {@link SearchExtBuilder}.
         *
         * @param name holds the names by which this search ext might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the search ext should use as its
         *        {@link NamedWriteable#getWriteableName()} too.  It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this search ext's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser function that reads the search ext builder from xcontent
         */
        public SearchExtSpec(
            ParseField name,
            Writeable.Reader<? extends T> reader,
            CheckedFunction<XContentParser, T, IOException> parser
        ) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link SearchExtBuilder}.
         *
         * @param name the name by which this search ext might be parsed or deserialized. Make sure that the search ext builder returns this name for
         *        {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param reader the reader registered for this search ext's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser function that reads the search ext builder from xcontent
         */
        public SearchExtSpec(String name, Writeable.Reader<? extends T> reader, CheckedFunction<XContentParser, T, IOException> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification for a {@link RescorerBuilder}.
     */
    class RescorerSpec<T extends RescorerBuilder<T>> extends SearchExtensionSpec<T, CheckedFunction<XContentParser, T, IOException>> {
        /**
         * Specification of custom {@link RescorerBuilder}.
         *
         * @param name holds the names by which this rescorer might be parsed. The {@link ParseField#getPreferredName()} is special as it
         *        is the name by under which the reader is registered. So it is the name that the rescorer should use as its
         *        {@link NamedWriteable#getWriteableName()} too.  It is an error if {@link ParseField#getPreferredName()} conflicts with
         *        another registered name, including names from other plugins.
         * @param reader the reader registered for this rescorer's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser function that reads the rescorer builder from xcontent
         */
        public RescorerSpec(ParseField name, Writeable.Reader<? extends T> reader, CheckedFunction<XContentParser, T, IOException> parser) {
            super(name, reader, parser);
        }

        /**
         * Specification of custom {@link RescorerBuilder}.
         *
         * @param name the name by which this rescorer might be parsed or deserialized. Make sure that the rescorer builder returns this name for
         *        {@link NamedWriteable#getWriteableName()}.  It is an error if this name conflicts with another registered name, including
         *        names from other plugins.
         * @param reader the reader registered for this rescorer's builder. Typically, a reference to a constructor that takes a
         *        {@link StreamInput}
         * @param parser the parser function that reads the rescorer builder from xcontent
         */
        public RescorerSpec(String name, Writeable.Reader<? extends T> reader, CheckedFunction<XContentParser, T, IOException> parser) {
            super(name, reader, parser);
        }
    }

    /**
     * Specification of search time behavior extension like a custom {@link MovAvgModel} or {@link ScoreFunction}.
     *
     * @param <W> the type of the main {@link NamedWriteable} for this spec. All specs have this but it isn't always *for* the same thing
     *        though, usually it is some sort of builder sent from the coordinating node to the data nodes executing the behavior
     * @param <P> the type of the parser for this spec. The parser runs on the coordinating node, converting {@link XContent} into the
     *        behavior to execute
     */
    class SearchExtensionSpec<W extends NamedWriteable, P> {
        private final ParseField name;
        private final Writeable.Reader<? extends W> reader;
        private final P parser;

        /**
         * Build the spec with a {@linkplain ParseField}.
         *
         * @param name the name of the behavior as a {@linkplain ParseField}. The parser is registered under all names specified by the
         *        {@linkplain ParseField} but the reader is only registered under the {@link ParseField#getPreferredName()} so be sure that
         *        that is the name that W's {@link NamedWriteable#getWriteableName()} returns.  It is an error if
         *        {@link ParseField#getPreferredName()} conflicts with another registered name, including names from other plugins.
         * @param reader reader that reads the behavior from the internode protocol
         * @param parser parser that read the behavior from a REST request
         */
        public SearchExtensionSpec(ParseField name, Writeable.Reader<? extends W> reader, P parser) {
            this.name = name;
            this.reader = reader;
            this.parser = parser;
        }

        /**
         * Build the spec with a String.
         *
         * @param name the name of the behavior. The parser and the reader are are registered under this name so be sure that that is the
         *        name that W's {@link NamedWriteable#getWriteableName()} returns.  It is an error if this name conflicts with another
         *        registered name, including names from other plugins.
         * @param reader reader that reads the behavior from the internode protocol
         * @param parser parser that read the behavior from a REST request
         */
        public SearchExtensionSpec(String name, Writeable.Reader<? extends W> reader, P parser) {
            this(new ParseField(name), reader, parser);
        }

        /**
         * The name of the thing being specified as a {@link ParseField}. This allows it to have deprecated names.
         */
        public ParseField getName() {
            return name;
        }

        /**
         * The reader responsible for reading the behavior from the internode protocol.
         */
        public Writeable.Reader<? extends W> getReader() {
            return reader;
        }

        /**
         * The parser responsible for converting {@link XContent} into the behavior.
         */
        public P getParser() {
            return parser;
        }
    }

    /**
     * Context available during fetch phase construction.
     */
    class FetchPhaseConstructionContext {
        private final Map<String, Highlighter> highlighters;

        public FetchPhaseConstructionContext(Map<String, Highlighter> highlighters) {
            this.highlighters = highlighters;
        }

        public Map<String, Highlighter> getHighlighters() {
            return highlighters;
        }
    }
}
