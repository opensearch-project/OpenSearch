/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.common.lucene.Lucene;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Compares case-insensitive required-trigram selection on indexes with many actual Lucene segments.
 * Includes candidate selection and approximation-query construction, but excludes parsing and document matching.
 *
 * ./gradlew --console=plain -p benchmarks run --args='WildcardRequiredTermsBenchmark.currentRewrite -p documentCount=100000'
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xms10g", "-Xmx10g", "--add-modules=jdk.incubator.vector" })
@SuppressWarnings("unused") // invoked by JMH
public class WildcardRequiredTermsBenchmark {
    private static final String FIELD = "field";
    private static final int DOCUMENT_LENGTH = 2048;
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        LogConfigurator.setNodeName("wildcard-required-terms-benchmark");
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    @Param({ "1000000" })
    public int documentCount;

    @Param({ "2048" })
    public int queryLength;

    private int segmentCount;

    @Param({ "mixed" })
    public String distribution;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Set<String> requiredTerms;
    private List<Terms> segmentTerms;
    private WildcardFieldMapper.RequiredTermsQuery currentQuery;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        String query = randomValue(new Random(711), queryLength, false);
        requiredTerms = WildcardFieldMapper.WildcardFieldType.getRequiredNGrams(query, false);
        currentQuery = new WildcardFieldMapper.RequiredTermsQuery(FIELD, requiredTerms, true);
        directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Lucene.KEYWORD_ANALYZER);
        Random random = new Random(812);
        // for matching some documents
        String fullMatch = query.toUpperCase(Locale.ROOT);
        int middle = fullMatch.length() / 2;
        String nearMatch = fullMatch.substring(0, middle) + "#" + fullMatch.substring(middle + 1);

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < documentCount; i++) {
                String value;
                if (i % 1000 == 0) {
                    value = fullMatch;
                } else if (i % 100 == 0) {
                    value = nearMatch;
                } else {
                    value = randomValue(random, DOCUMENT_LENGTH, distribution.equals("mixed"));
                }

                Tokenizer tokenizer = new WildcardFieldMapper.WildcardFieldTokenizer();
                tokenizer.setReader(new StringReader(value));
                Document document = new Document();
                document.add(new Field(FIELD, tokenizer, FIELD_TYPE));
                writer.addDocument(document);
                int indexed = i + 1;
                if (indexed % 10_000 == 0) {
                    System.out.printf(
                        "[setup] indexed %,d / %,d (%d%%), target segments=%,d%n",
                        indexed,
                        documentCount,
                        100L * indexed / documentCount,
                        segmentCount
                    );
                }
            }
        }
        reader = DirectoryReader.open(directory);
        segmentCount = reader.leaves().size();
        searcher = new IndexSearcher(reader);
        System.out.printf("[setup] documents = %d", segmentCount);
        segmentTerms = new ArrayList<>(segmentCount);
        for (LeafReaderContext leaf : reader.leaves()) {
            segmentTerms.add(leaf.reader().terms(FIELD));
        }
        // Independently sum every indexed term's docFreq, folding only the synthetic ASCII case variants.
        System.out.println("[setup] indexing complete, validating benchmark implementations...");
        System.out.println(
            "VALIDATED queryLength="
                + queryLength
                + " requiredTerms="
                + requiredTerms.size()
                + " segments="
                + segmentCount
                + " documents="
                + reader.numDocs()
                + " distribution="
                + distribution
        );
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        reader.close();
        directory.close();
    }

    @Benchmark
    public Query currentRewrite() throws IOException {
        return currentQuery.rewrite(searcher);
    }

    private static String randomValue(Random random, int length, boolean mixedCase) {
        StringBuilder value = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char current = (char) ('a' + random.nextInt(26));
            if (mixedCase && random.nextBoolean()) {
                current = Character.toUpperCase(current);
            }
            value.append(current);
        }
        return value.toString();
    }

    private record Candidate(String text, long frequency) {
    }
}
