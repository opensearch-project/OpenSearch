/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.codecs.asserting.AssertingDocValuesFormat;
import org.apache.lucene.tests.codecs.asserting.AssertingPostingsFormat;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.fuzzy.FuzzyFilterPostingsFormat;
import org.opensearch.index.codec.fuzzy.FuzzySetFactory;
import org.opensearch.index.codec.fuzzy.FuzzySetParameters;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@SuppressCodecs("*") // the per-field formats are asserted against the default codec ones
public class PerFieldCodecTests extends OpenSearchTestCase {
    private static final List<String> OPENSEARCH_CODECS = List.of(
        CodecService.DEFAULT_CODEC,
        CodecService.LZ4,
        CodecService.BEST_COMPRESSION_CODEC,
        CodecService.ZLIB
    );
    private static final String CLAIMED_FIELD = "claimed";
    private static final String OTHER_FIELD = "other";

    public void testCodecIsNotWrappedWithoutContributions() {
        final Codec delegate = new Lucene104Codec();
        final DelegatingPerFieldCodecBuilder builder = new DelegatingPerFieldCodecBuilder(List.of());

        assertTrue(builder.isEmpty());
        assertSame(delegate, builder.build(delegate));
    }

    public void testCodecIsNotWrappedWhenBuildersContributeNothing() {
        final Codec delegate = new Lucene104Codec();
        final DelegatingPerFieldCodecBuilder builder = new DelegatingPerFieldCodecBuilder(List.of(new PerFieldCodecBuilder() {
        }));

        assertTrue(builder.isEmpty());
        assertSame(delegate, builder.build(delegate));
    }

    public void testFormatsWithoutContributionsAreLeftIntact() {
        final Lucene104Codec delegate = new Lucene104Codec();
        final Codec codec = wrap(delegate, postingsBuilder(new Lucene104PostingsFormat()));

        assertSame(delegate.docValuesFormat(), codec.docValuesFormat());
        assertSame(delegate.knnVectorsFormat(), codec.knnVectorsFormat());
        assertSame(delegate.storedFieldsFormat(), codec.storedFieldsFormat());
    }

    public void testContributionsToCodecWithoutPerFieldSupportAreRejected() {
        final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();
        final Codec delegate = new FilterCodec("Lucene104", new Lucene104Codec()) {
            @Override
            public DocValuesFormat docValuesFormat() {
                return docValuesFormat;
            }
        };

        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> wrap(delegate, docValuesBuilder(new Lucene90DocValuesFormat()))
        );

        assertThat(ex.getMessage(), containsString("does not support per-field doc values customization"));

        // the formats which are per-field aware are still customizable
        final Codec codec = wrap(delegate, postingsBuilder(new Lucene104PostingsFormat()));
        assertSame(docValuesFormat, codec.docValuesFormat());
    }

    public void testCodecReportsDelegateName() {
        final Codec delegate = new Lucene104Codec();
        final Codec codec = wrap(delegate, postingsBuilder(new Lucene104PostingsFormat()));

        assertThat(codec, instanceOf(CompositeDelegatingPerFieldCodec.class));
        assertEquals(delegate.getName(), codec.getName());
    }

    public void testClaimedFieldUsesContributedPostingsFormat() {
        final PostingsFormat contributed = new Lucene104PostingsFormat();
        final Lucene104Codec delegate = new Lucene104Codec();
        final PostingsFormat postingsFormat = wrap(delegate, postingsBuilder(contributed)).postingsFormat();

        assertThat(postingsFormat, instanceOf(PerFieldPostingsFormat.class));
        final PerFieldPostingsFormat perField = (PerFieldPostingsFormat) postingsFormat;
        assertSame(contributed, perField.getPostingsFormatForField(CLAIMED_FIELD));

        // not claimed: the format of the delegate is used, unwrapped so the per-field bookkeeping is not nested
        final PostingsFormat fallback = perField.getPostingsFormatForField(OTHER_FIELD);
        assertSame(delegate.getPostingsFormatForField(OTHER_FIELD), fallback);
        assertThat(fallback, not(instanceOf(PerFieldPostingsFormat.class)));
    }

    public void testClaimedFieldUsesContributedDocValuesFormat() {
        final DocValuesFormat contributed = new Lucene90DocValuesFormat();
        final Lucene104Codec delegate = new Lucene104Codec();
        final DocValuesFormat docValuesFormat = wrap(delegate, docValuesBuilder(contributed)).docValuesFormat();

        assertThat(docValuesFormat, instanceOf(PerFieldDocValuesFormat.class));
        final PerFieldDocValuesFormat perField = (PerFieldDocValuesFormat) docValuesFormat;
        assertSame(contributed, perField.getDocValuesFormatForField(CLAIMED_FIELD));

        final DocValuesFormat fallback = perField.getDocValuesFormatForField(OTHER_FIELD);
        assertSame(delegate.getDocValuesFormatForField(OTHER_FIELD), fallback);
        assertThat(fallback, not(instanceOf(PerFieldDocValuesFormat.class)));
    }

    public void testClaimedFieldUsesContributedKnnVectorsFormat() {
        final KnnVectorsFormat contributed = new Lucene99HnswVectorsFormat();
        final Lucene104Codec delegate = new Lucene104Codec();
        final KnnVectorsFormat knnVectorsFormat = wrap(delegate, knnVectorsBuilder(contributed)).knnVectorsFormat();

        assertThat(knnVectorsFormat, instanceOf(PerFieldKnnVectorsFormat.class));
        final PerFieldKnnVectorsFormat perField = (PerFieldKnnVectorsFormat) knnVectorsFormat;
        assertSame(contributed, perField.getKnnVectorsFormatForField(CLAIMED_FIELD));

        final KnnVectorsFormat fallback = perField.getKnnVectorsFormatForField(OTHER_FIELD);
        assertSame(delegate.getKnnVectorsFormatForField(OTHER_FIELD), fallback);
        assertThat(fallback, not(instanceOf(PerFieldKnnVectorsFormat.class)));
    }

    public void testContributionsOfSeveralPluginsAreComposed() {
        final PostingsFormat postings = new Lucene104PostingsFormat();
        final KnnVectorsFormat knnVectors = new Lucene99HnswVectorsFormat();

        final Codec codec = wrap(new Lucene104Codec(), postingsBuilder(postings), knnVectorsBuilder(knnVectors));
        assertSame(postings, ((PerFieldPostingsFormat) codec.postingsFormat()).getPostingsFormatForField(CLAIMED_FIELD));
        assertSame(knnVectors, ((PerFieldKnnVectorsFormat) codec.knnVectorsFormat()).getKnnVectorsFormatForField(CLAIMED_FIELD));
    }

    public void testConflictingPostingsFormatContributions() {
        final PerFieldCodecBuilder first = postingsBuilder(new Lucene104PostingsFormat());
        final PerFieldCodecBuilder second = postingsBuilder(new Lucene104PostingsFormat());
        final PostingsFormat postingsFormat = wrap(new Lucene104Codec(), first, second).postingsFormat();

        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> ((PerFieldPostingsFormat) postingsFormat).getPostingsFormatForField(CLAIMED_FIELD)
        );

        assertThat(ex.getMessage(), containsString("postings format for field [" + CLAIMED_FIELD + "]"));
        assertThat(ex.getMessage(), containsString(first.getClass().getName()));
    }

    public void testConflictingKnnVectorsFormatContributions() {
        final KnnVectorsFormat knnVectorsFormat = wrap(
            new Lucene104Codec(),
            knnVectorsBuilder(new Lucene99HnswVectorsFormat()),
            knnVectorsBuilder(new Lucene99HnswVectorsFormat())
        ).knnVectorsFormat();

        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> ((PerFieldKnnVectorsFormat) knnVectorsFormat).getKnnVectorsFormatForField(CLAIMED_FIELD)
        );

        assertThat(ex.getMessage(), containsString("knn vectors format for field [" + CLAIMED_FIELD + "]"));
    }

    public void testConflictingDocValuesFormatContributions() {
        final DocValuesFormat docValuesFormat = wrap(
            new Lucene104Codec(),
            docValuesBuilder(new Lucene90DocValuesFormat()),
            docValuesBuilder(new Lucene90DocValuesFormat())
        ).docValuesFormat();

        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> ((PerFieldDocValuesFormat) docValuesFormat).getDocValuesFormatForField(CLAIMED_FIELD)
        );

        assertThat(ex.getMessage(), containsString("doc values format for field [" + CLAIMED_FIELD + "]"));
    }

    public void testConflictingClaimsAreRejectedEagerly() {
        final PerFieldCodecBuilder first = postingsBuilder(new Lucene104PostingsFormat());
        final DelegatingPerFieldCodecBuilder builder = new DelegatingPerFieldCodecBuilder(
            List.of(first, docValuesBuilder(new Lucene90DocValuesFormat()), postingsBuilder(new Lucene104PostingsFormat()))
        );

        // reported as an invalid mapping, not as a failure to write the segment
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> builder.validate(List.of(OTHER_FIELD, CLAIMED_FIELD))
        );
        assertThat(ex.getMessage(), containsString("postings format for field [" + CLAIMED_FIELD + "]"));
        assertThat(ex.getMessage(), containsString(first.getClass().getName()));
    }

    public void testClaimsOfDifferentFieldsPassValidation() {
        final DelegatingPerFieldCodecBuilder builder = new DelegatingPerFieldCodecBuilder(
            List.of(
                postingsBuilder(Map.of(CLAIMED_FIELD, new Lucene104PostingsFormat())),
                postingsBuilder(Map.of(OTHER_FIELD, new Lucene104PostingsFormat()))
            )
        );
        builder.validate(List.of(CLAIMED_FIELD, OTHER_FIELD, "unclaimed"));
    }

    public void testContributionsOfDifferentPluginsForDifferentFieldsDoNotConflict() {
        final PostingsFormat first = new Lucene104PostingsFormat();
        final PostingsFormat second = new Lucene104PostingsFormat();

        final Codec codec = wrap(
            new Lucene104Codec(),
            postingsBuilder(Map.of(CLAIMED_FIELD, first)),
            postingsBuilder(Map.of(OTHER_FIELD, second))
        );

        final PerFieldPostingsFormat perField = (PerFieldPostingsFormat) codec.postingsFormat();
        assertSame(first, perField.getPostingsFormatForField(CLAIMED_FIELD));
        assertSame(second, perField.getPostingsFormatForField(OTHER_FIELD));
    }

    public void testCodecServiceAppliesContributionsToCodecs() {
        final CodecService codecService = codecService(List.of(postingsBuilder(new Lucene104PostingsFormat())));

        for (final String name : OPENSEARCH_CODECS) {
            assertThat("codec [" + name + "]", codecService.codec(name), instanceOf(CompositeDelegatingPerFieldCodec.class));
        }

        assertThat(codecService.defaultCodec(), instanceOf(CompositeDelegatingPerFieldCodec.class));
        // the contributions are injected on demand: the codecs which do not support them (any of the codecs
        // registered through the service loader, for example) are only rejected if the index does use them
        assertSame(codecService.codec(CodecService.DEFAULT_CODEC), codecService.codec(CodecService.DEFAULT_CODEC));
    }

    public void testCodecServiceKeepsCodecsIntactWithoutContributions() {
        final CodecService codecService = codecService(List.of());

        for (final String name : codecService.availableCodecs()) {
            assertThat("codec [" + name + "]", codecService.codec(name), not(instanceOf(CompositeDelegatingPerFieldCodec.class)));
        }
    }

    /**
     * The segments written with the contributed formats have to be readable by the delegate codec alone: the
     * per-field formats are resolved from the field attributes through the service loader at read time.
     */
    public void testSegmentsWrittenWithContributedFormatsAreReadableByDelegate() throws Exception {
        final String vectorField = "vector";
        final String fuzzyField = "fuzzy";
        final int dimensions = 64;

        final PostingsFormat postingsFormat = new FuzzyFilterPostingsFormat(
            new Lucene104PostingsFormat(),
            new FuzzySetFactory(Map.of(fuzzyField, new FuzzySetParameters(() -> 0.01)))
        );
        final KnnVectorsFormat knnVectorsFormat = new Lucene104HnswScalarQuantizedVectorsFormat();

        final Codec codec = wrap(
            new Lucene104Codec(),
            postingsBuilder(Map.of(fuzzyField, postingsFormat)),
            knnVectorsBuilder(Map.of(vectorField, knnVectorsFormat))
        );

        final float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; ++i) {
            vector[i] = i;
        }

        try (Directory directory = newDirectory()) {
            final IndexWriterConfig config = newIndexWriterConfig(null);
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                final Document document = new Document();
                document.add(new StringField(fuzzyField, "fuzzy-value", Field.Store.NO));
                document.add(new StringField(OTHER_FIELD, "other-value", Field.Store.NO));
                document.add(new KnnFloatVectorField(vectorField, vector));
                writer.addDocument(document);
                writer.commit();
            }

            // the codec is resolved from the segment infos through the service loader, no wrapping involved
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final LeafReader leafReader = reader.leaves().get(0).reader();
                final SegmentReader segmentReader = (SegmentReader) leafReader;
                assertEquals(codec.getName(), segmentReader.getSegmentInfo().info.getCodec().getName());
                assertThat(segmentReader.getSegmentInfo().info.getCodec(), not(instanceOf(CompositeDelegatingPerFieldCodec.class)));

                final FieldInfo fuzzyFieldInfo = leafReader.getFieldInfos().fieldInfo(fuzzyField);
                assertEquals(
                    FuzzyFilterPostingsFormat.FUZZY_FILTER_CODEC_NAME,
                    fuzzyFieldInfo.getAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY)
                );

                final FieldInfo vectorFieldInfo = leafReader.getFieldInfos().fieldInfo(vectorField);
                assertEquals(knnVectorsFormat.getName(), vectorFieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY));

                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(1, searcher.count(new TermQuery(new Term(fuzzyField, "fuzzy-value"))));
                assertEquals(1, searcher.count(new TermQuery(new Term(OTHER_FIELD, "other-value"))));

                final FloatVectorValues vectorValues = leafReader.getFloatVectorValues(vectorField);
                assertNotNull(vectorValues);
                assertEquals(1, vectorValues.size());
                assertEquals(dimensions, vectorValues.dimension());
            }
        }
    }

    /**
     * The scenario the per-field contributions are meant to support: two plugins, each one owning its own
     * fields of the very same index and encoding them with its own formats (the case which is not possible
     * with {@link CodecServiceFactory} since only one plugin per index could provide it).
     */
    public void testDifferentPluginsOwnDifferentFieldsOfSameIndex() throws Exception {
        final int dimensions = 64;

        // the fields (and formats) of the first plugin
        final String firstTerms = "first_terms";
        final String firstDocValues = "first_doc_values";
        final String firstVector = "first_vector";
        final PostingsFormat firstPostingsFormat = new FuzzyFilterPostingsFormat(
            new Lucene104PostingsFormat(),
            new FuzzySetFactory(Map.of(firstTerms, new FuzzySetParameters(() -> 0.01)))
        );
        final DocValuesFormat firstDocValuesFormat = new Lucene90DocValuesFormat();
        final KnnVectorsFormat firstKnnVectorsFormat = new Lucene99HnswVectorsFormat();

        // the fields (and formats) of the second plugin
        final String secondTerms = "second_terms";
        final String secondDocValues = "second_doc_values";
        final String secondVector = "second_vector";
        final PostingsFormat secondPostingsFormat = new AssertingPostingsFormat();
        final DocValuesFormat secondDocValuesFormat = new AssertingDocValuesFormat();
        final KnnVectorsFormat secondKnnVectorsFormat = new Lucene104HnswScalarQuantizedVectorsFormat();

        // every format of the two plugins is distinct from the one of the other plugin, and from the default ones
        final Map<String, String> expectedPostingsFormats = Map.of(
            firstTerms,
            firstPostingsFormat.getName(),
            secondTerms,
            secondPostingsFormat.getName(),
            OTHER_FIELD,
            new Lucene104Codec().getPostingsFormatForField(OTHER_FIELD).getName()
        );
        final Map<String, String> expectedDocValuesFormats = Map.of(
            firstDocValues,
            firstDocValuesFormat.getName(),
            secondDocValues,
            secondDocValuesFormat.getName()
        );
        final Map<String, String> expectedKnnVectorsFormats = Map.of(
            firstVector,
            firstKnnVectorsFormat.getName(),
            secondVector,
            secondKnnVectorsFormat.getName()
        );
        assertNotEquals(firstPostingsFormat.getName(), secondPostingsFormat.getName());
        assertNotEquals(firstDocValuesFormat.getName(), secondDocValuesFormat.getName());
        assertNotEquals(firstKnnVectorsFormat.getName(), secondKnnVectorsFormat.getName());

        final Codec codec = wrap(
            new Lucene104Codec(),
            pluginBuilder(
                Map.of(firstTerms, firstPostingsFormat),
                Map.of(firstDocValues, firstDocValuesFormat),
                Map.of(firstVector, firstKnnVectorsFormat)
            ),
            pluginBuilder(
                Map.of(secondTerms, secondPostingsFormat),
                Map.of(secondDocValues, secondDocValuesFormat),
                Map.of(secondVector, secondKnnVectorsFormat)
            )
        );

        final float[] firstVectorValue = new float[dimensions];
        final float[] secondVectorValue = new float[dimensions];
        for (int i = 0; i < dimensions; ++i) {
            firstVectorValue[i] = i;
            secondVectorValue[i] = dimensions - i;
        }

        try (Directory directory = newDirectory()) {
            final IndexWriterConfig config = newIndexWriterConfig(null);
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                final Document document = new Document();
                document.add(new StringField(firstTerms, "first-value", Field.Store.NO));
                document.add(new StringField(secondTerms, "second-value", Field.Store.NO));
                document.add(new StringField(OTHER_FIELD, "other-value", Field.Store.NO));
                document.add(new NumericDocValuesField(firstDocValues, 1L));
                document.add(new NumericDocValuesField(secondDocValues, 2L));
                document.add(new KnnFloatVectorField(firstVector, firstVectorValue));
                document.add(new KnnFloatVectorField(secondVector, secondVectorValue));
                writer.addDocument(document);
                writer.commit();
            }

            // the codec is resolved from the segment infos through the service loader, no wrapping involved
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final LeafReader leafReader = reader.leaves().get(0).reader();
                assertThat(
                    ((SegmentReader) leafReader).getSegmentInfo().info.getCodec(),
                    not(instanceOf(CompositeDelegatingPerFieldCodec.class))
                );

                assertFormats(leafReader, PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY, expectedPostingsFormats);
                assertFormats(leafReader, PerFieldDocValuesFormat.PER_FIELD_FORMAT_KEY, expectedDocValuesFormats);
                assertFormats(leafReader, PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY, expectedKnnVectorsFormats);

                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(1, searcher.count(new TermQuery(new Term(firstTerms, "first-value"))));
                assertEquals(1, searcher.count(new TermQuery(new Term(secondTerms, "second-value"))));
                assertEquals(1, searcher.count(new TermQuery(new Term(OTHER_FIELD, "other-value"))));

                assertDocValue(leafReader, firstDocValues, 1L);
                assertDocValue(leafReader, secondDocValues, 2L);

                assertVectorValue(leafReader, firstVector, firstVectorValue);
                assertVectorValue(leafReader, secondVector, secondVectorValue);
            }
        }
    }

    private static void assertFormats(LeafReader reader, String attribute, Map<String, String> expected) {
        for (final Map.Entry<String, String> field : expected.entrySet()) {
            final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field.getKey());
            assertNotNull("field [" + field.getKey() + "]", fieldInfo);
            assertEquals("format of field [" + field.getKey() + "]", field.getValue(), fieldInfo.getAttribute(attribute));
        }
    }

    private static void assertDocValue(LeafReader reader, String field, long expected) throws Exception {
        final NumericDocValues docValues = reader.getNumericDocValues(field);
        assertNotNull("doc values of field [" + field + "]", docValues);
        assertTrue(docValues.advanceExact(0));
        assertEquals(expected, docValues.longValue());
    }

    private static void assertVectorValue(LeafReader reader, String field, float[] expected) throws Exception {
        final FloatVectorValues vectorValues = reader.getFloatVectorValues(field);
        assertNotNull("vectors of field [" + field + "]", vectorValues);
        assertEquals(1, vectorValues.size());
        assertEquals(expected.length, vectorValues.dimension());

        final TopDocs topDocs = reader.searchNearestVectors(
            field,
            expected,
            1,
            AcceptDocs.fromLiveDocs(reader.getLiveDocs(), reader.maxDoc()),
            Integer.MAX_VALUE
        );
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(0, topDocs.scoreDocs[0].doc);
    }

    private static Codec wrap(Codec delegate, PerFieldCodecBuilder... builders) {
        return new DelegatingPerFieldCodecBuilder(List.of(builders)).build(delegate);
    }

    private static PerFieldCodecBuilder pluginBuilder(
        Map<String, PostingsFormat> postings,
        Map<String, DocValuesFormat> docValues,
        Map<String, KnnVectorsFormat> knnVectors
    ) {
        final PerFieldPostingsFormat perFieldPostings = new PerFieldPostingsFormat() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
                return postings.get(field);
            }
        };

        final PerFieldDocValuesFormat perFieldDocValues = new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValues.get(field);
            }
        };

        final PerFieldKnnVectorsFormat perFieldKnnVectors = new PerFieldKnnVectorsFormat() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return knnVectors.get(field);
            }
        };

        return new PerFieldCodecBuilder() {
            @Override
            public Optional<PerFieldPostingsFormat> postingsFormat() {
                return postings.isEmpty() ? Optional.empty() : Optional.of(perFieldPostings);
            }

            @Override
            public Optional<PerFieldDocValuesFormat> docValuesFormat() {
                return docValues.isEmpty() ? Optional.empty() : Optional.of(perFieldDocValues);
            }

            @Override
            public Optional<PerFieldKnnVectorsFormat> knnVectorsFormat() {
                return knnVectors.isEmpty() ? Optional.empty() : Optional.of(perFieldKnnVectors);
            }
        };
    }

    private CodecService codecService(List<PerFieldCodecBuilder> builders) {
        final Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        return new CodecService(null, indexSettings, LogManager.getLogger("test"), List.of(), builders);
    }

    private static PerFieldCodecBuilder postingsBuilder(PostingsFormat format) {
        return postingsBuilder(Map.of(CLAIMED_FIELD, format));
    }

    private static PerFieldCodecBuilder postingsBuilder(Map<String, PostingsFormat> formats) {
        final PerFieldPostingsFormat perField = new PerFieldPostingsFormat() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
                return formats.get(field);
            }
        };

        return new PerFieldCodecBuilder() {
            @Override
            public Optional<PerFieldPostingsFormat> postingsFormat() {
                return Optional.of(perField);
            }
        };
    }

    private static PerFieldCodecBuilder docValuesBuilder(DocValuesFormat format) {
        return docValuesBuilder(Map.of(CLAIMED_FIELD, format));
    }

    private static PerFieldCodecBuilder docValuesBuilder(Map<String, DocValuesFormat> formats) {
        final PerFieldDocValuesFormat perField = new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return formats.get(field);
            }
        };

        return new PerFieldCodecBuilder() {
            @Override
            public Optional<PerFieldDocValuesFormat> docValuesFormat() {
                return Optional.of(perField);
            }
        };
    }

    private static PerFieldCodecBuilder knnVectorsBuilder(KnnVectorsFormat format) {
        return knnVectorsBuilder(Map.of(CLAIMED_FIELD, format));
    }

    private static PerFieldCodecBuilder knnVectorsBuilder(Map<String, KnnVectorsFormat> formats) {
        final PerFieldKnnVectorsFormat perField = new PerFieldKnnVectorsFormat() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return formats.get(field);
            }
        };

        return new PerFieldCodecBuilder() {
            @Override
            public Optional<PerFieldKnnVectorsFormat> knnVectorsFormat() {
                return Optional.of(perField);
            }
        };
    }
}
