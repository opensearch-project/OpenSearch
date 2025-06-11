/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AggregatorCancellationTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new SearchPlugin() {
            @Override
            public List<AggregationSpec> getAggregations() {
                return List.of(
                    new AggregationSpec(
                        "custom_cancellable",
                        CustomCancellableAggregationBuilder::new,
                        CustomCancellableAggregationBuilder.PARSER
                    )
                );
            }
        });
    }

    public void testNestedAggregationCancellation() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

            // Create documents
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("cat" + (i % 10))));
                doc.add(new SortedSetDocValuesField("subcategory", new BytesRef("subcat" + (i % 50))));
                doc.add(new SortedSetDocValuesField("brand", new BytesRef("brand" + (i % 20))));
                doc.add(new SortedNumericDocValuesField("value", i));
                indexWriter.addDocument(doc);
            }
            indexWriter.close();

            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                // Create nested aggregations with our custom cancellable agg
                CustomCancellableAggregationBuilder aggBuilder = new CustomCancellableAggregationBuilder("test_agg").subAggregation(
                    new TermsAggregationBuilder("categories").field("category")
                        .size(10)
                        .subAggregation(
                            new TermsAggregationBuilder("subcategories").field("subcategory")
                                .size(50000)
                                .subAggregation(new TermsAggregationBuilder("brands").field("brand").size(20000))
                        )
                );

                expectThrows(
                    OpenSearchRejectedExecutionException.class,
                    () -> searchAndReduce(
                        searcher,
                        new MatchAllDocsQuery(),
                        aggBuilder,
                        keywordField("category"),
                        keywordField("subcategory"),
                        keywordField("brand")
                    )
                );
            }
        }
    }

    private static class CustomCancellableAggregationBuilder extends AbstractAggregationBuilder<CustomCancellableAggregationBuilder> {

        public static final String NAME = "custom_cancellable";

        public static final ObjectParser<CustomCancellableAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
            NAME,
            CustomCancellableAggregationBuilder::new
        );

        CustomCancellableAggregationBuilder(String name) {
            super(name);
        }

        public CustomCancellableAggregationBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected AggregatorFactory doBuild(
            QueryShardContext queryShardContext,
            AggregatorFactory parent,
            AggregatorFactories.Builder subfactoriesBuilder
        ) throws IOException {
            return new AggregatorFactory(name, queryShardContext, parent, subfactoriesBuilder, metadata) {
                @Override
                protected Aggregator createInternal(
                    SearchContext searchContext,
                    Aggregator parent,
                    CardinalityUpperBound cardinality,
                    Map<String, Object> metadata
                ) throws IOException {
                    return new CustomCancellableAggregator(
                        name,
                        searchContext,
                        parent,
                        subfactoriesBuilder.build(queryShardContext, this),
                        null
                    );
                }
            };
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public CustomCancellableAggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            return new CustomCancellableAggregationBuilder(getName()).setMetadata(metadata).subAggregations(factoriesBuilder);
        }

        public String getType() {
            return NAME;
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            // Nothing to write
        }
    }

    private static class CustomCancellableAggregator extends AggregatorBase {

        CustomCancellableAggregator(
            String name,
            SearchContext context,
            Aggregator parent,
            AggregatorFactories factories,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, context, parent, CardinalityUpperBound.NONE, metadata);
        }

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            checkCancelled();
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        protected void checkCancelled() {
            throw new OpenSearchRejectedExecutionException("The request has been cancelled");
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            InternalAggregation internalAggregation = new InternalSum(name(), 0.0, DocValueFormat.RAW, metadata());
            return new InternalAggregation[] { internalAggregation };
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return new InternalSum(name(), 0.0, DocValueFormat.RAW, metadata());
        }
    }
}
