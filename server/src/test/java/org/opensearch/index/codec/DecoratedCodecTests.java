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
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The per-segment formats ({@link CompoundFormat}, {@link StoredFieldsFormat}) of several plugins are
 * composed within the same index, the way {@code k-NN} and {@code neural-search} need it: both keep the
 * files of their native engines out of the compound file, and one of them (the derived source of
 * {@code k-NN}) customizes the stored fields.
 */
@SuppressCodecs("*")
public class DecoratedCodecTests extends OpenSearchTestCase {
    private static final String MARKER = "side-file-of-";
    private static final String EXTENSION_A = "tsfa";
    private static final String EXTENSION_B = "tsfb";
    private static final String STORED_FIELDS_PREFIX = "decorated:";
    private static final String COMPOUND_SUFFIX = "c";

    private static final String FIELD_A = "field_a";
    private static final String FIELD_B = "field_b";
    private static final String STORED_FIELD = "stored";

    public void testCodecIsNotDecoratedWithoutDecorators() {
        final Codec delegate = new Lucene104Codec();
        assertSame(delegate, DecoratedCodec.wrap(delegate, List.of()));
    }

    public void testDecoratedCodecIsNotSelectableByName() {
        assertThat(Codec.availableCodecs(), hasItem(Decorated104Codec.NAME));
        assertThat(Arrays.asList(codecService(List.of(), List.of()).availableCodecs()), not(hasItem(Decorated104Codec.NAME)));
    }

    public void testCodecServiceAppliesDecorators() {
        final CodecService codecService = codecService(List.of(), List.of(new SideFileDecoratorA()));

        assertThat(codecService.codec(CodecService.DEFAULT_CODEC), instanceOf(DecoratedCodec.class));
        assertThat(codecService.defaultCodec(), instanceOf(DecoratedCodec.class));
        assertEquals(Decorated104Codec.NAME, codecService.codec(CodecService.DEFAULT_CODEC).getName());
    }

    public void testStoredFieldsCouldOnlyBeCustomizedOnce() {
        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> DecoratedCodec.wrap(new Lucene104Codec(), List.of(new StoredFieldsDecorator(), new SecondStoredFieldsDecorator()))
        );

        // the decorators are applied ordered by name
        assertThat(
            ex.getMessage(),
            containsString("The stored fields format is already customized by [" + SecondStoredFieldsDecorator.NAME + "]")
        );
        assertThat(ex.getMessage(), containsString(StoredFieldsDecorator.NAME));
    }

    public void testDecoratorContributedTwiceIsRejected() {
        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> DecoratedCodec.wrap(new Lucene104Codec(), List.of(new SideFileDecoratorA(), new SideFileDecoratorA()))
        );
        assertThat(ex.getMessage(), containsString(SideFileDecoratorA.NAME));
    }

    /**
     * The write instance hands out the formats of the delegate themselves, so whatever wraps it and
     * inspects them keeps working.
     */
    public void testWriteInstanceExposesFormatsOfDelegate() {
        final Codec delegate = new Lucene104Codec();
        final Codec codec = DecoratedCodec.wrap(delegate, List.of(new StoredFieldsDecorator()));

        assertSame(delegate.postingsFormat(), codec.postingsFormat());
        assertSame(delegate.docValuesFormat(), codec.docValuesFormat());
        assertSame(delegate.knnVectorsFormat(), codec.knnVectorsFormat());
        // some of the formats are created on every call
        assertSame(delegate.pointsFormat().getClass(), codec.pointsFormat().getClass());
        assertSame(delegate.normsFormat().getClass(), codec.normsFormat().getClass());
        assertSame(delegate.termVectorsFormat().getClass(), codec.termVectorsFormat().getClass());
        assertSame(delegate.fieldInfosFormat().getClass(), codec.fieldInfosFormat().getClass());
        assertSame(delegate.liveDocsFormat().getClass(), codec.liveDocsFormat().getClass());
        assertSame(delegate.compoundFormat().getClass(), codec.compoundFormat().getClass());
        assertThat(codec.storedFieldsFormat(), instanceOf(PrefixingStoredFieldsFormat.class));
    }

    /**
     * The context aware segments wrap the codec of the index into the {@link CriteriaBasedCodec}, which only
     * records the bucket of the segment if the postings format it wraps is a per-field one.
     */
    public void testCriteriaBasedCodecOnTopOfDecoratedCodecRecordsBucket() throws Exception {
        final Codec codec = new CriteriaBasedCodec(DecoratedCodec.wrap(new Lucene104Codec(), List.of(new StoredFieldsDecorator())), "b1");

        try (Directory directory = newDirectory()) {
            final IndexWriterConfig config = new IndexWriterConfig(null);
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                final Document document = new Document();
                document.add(new StringField(CriteriaBasedCodec.ATTRIBUTE_BINDING_TARGET_FIELD, "1", Field.Store.NO));
                document.add(new StoredField(STORED_FIELD, "value"));
                writer.addDocument(document);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
                final SegmentInfo si = segmentReader.getSegmentInfo().info;

                assertEquals(Decorated104Codec.NAME, si.getCodec().getName());
                assertEquals("b1", si.getAttribute(CriteriaBasedCodec.BUCKET_NAME));
                assertEquals(
                    "b1",
                    segmentReader.getFieldInfos()
                        .fieldInfo(CriteriaBasedCodec.ATTRIBUTE_BINDING_TARGET_FIELD)
                        .getAttribute(CriteriaBasedCodec.BUCKET_NAME)
                );
                assertEquals("value", reader.storedFields().document(0).get(STORED_FIELD));
            }
        }
    }

    /**
     * The decorators get the delegate codec itself, so they could open its other structures (the way the derived
     * source reads the vectors back): while reading, it is the codec the segment records.
     */
    public void testDecoratorsGetDelegateCodecRecordedInSegment() throws Exception {
        final Codec delegate = new DelegateCodec();
        final Codec codec = DecoratedCodec.wrap(delegate, List.of(new DelegateCapturingDecorator()));

        try (Directory directory = newDirectory()) {
            final IndexWriterConfig config = new IndexWriterConfig(null);
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                final Document document = new Document();
                document.add(new StoredField(STORED_FIELD, "value"));
                writer.addDocument(document);
            }

            DelegateCapturingDecorator.READ_WITH.set(null);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals("value", reader.storedFields().document(0).get(STORED_FIELD));
                assertSame(Codec.forName(DelegateCodec.NAME), DelegateCapturingDecorator.READ_WITH.get());
            }
        }
    }

    public void testCodecWithIncompatibleSegmentInfoFormatIsRejected() {
        final SegmentInfoFormat incompatible = new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void write(Directory dir, SegmentInfo info, IOContext ioContext) {
                throw new UnsupportedOperationException();
            }
        };

        final Codec delegate = new FilterCodec("Lucene104", new Lucene104Codec()) {
            @Override
            public SegmentInfoFormat segmentInfoFormat() {
                return incompatible;
            }
        };

        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> DecoratedCodec.wrap(delegate, List.of(new SideFileDecoratorA()))
        );
        assertThat(ex.getMessage(), containsString("could not be decorated: it uses the segment info format"));
    }

    public void testSegmentWithoutRecordedDelegateIsRejected() throws Exception {
        final Codec codec = Codec.forName(Decorated104Codec.NAME);

        try (Directory directory = newDirectory()) {
            final SegmentInfo si = segmentInfo(directory, codec, Map.of());
            final IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                () -> codec.compoundFormat().getCompoundReader(directory, si)
            );
            assertThat(ex.getMessage(), containsString("does not record the delegate codec"));
        }
    }

    public void testSegmentWrittenByUnavailableDecoratorIsRejected() throws Exception {
        final Codec codec = Codec.forName(Decorated104Codec.NAME);

        try (Directory directory = newDirectory()) {
            final SegmentInfo si = segmentInfo(
                directory,
                codec,
                Map.of(DecoratedCodec.DELEGATE_ATTRIBUTE, "Lucene104", DecoratedCodec.DECORATORS_ATTRIBUTE, "ThereIsNoSuchDecorator")
            );

            final IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                () -> codec.compoundFormat().getCompoundReader(directory, si)
            );
            assertThat(ex.getMessage(), containsString("codec decorator [ThereIsNoSuchDecorator] which is not available"));
        }
    }

    /**
     * Two plugins keep the files of their own "native engines" out of the compound file while a third one
     * customizes the stored fields: the segment records everything needed to reconstruct the formats, so
     * the codec created by the service loader alone is able to read it back.
     */
    public void testSeveralPluginsCustomizePerSegmentFormatsOfSameIndex() throws Exception {
        final Codec delegate = new Lucene104Codec(Lucene104Codec.Mode.BEST_COMPRESSION);
        final Codec codec = DecoratedCodec.wrap(
            // the per-field formats create the side files, the way the native engines of the plugins do
            new DelegatingPerFieldCodecBuilder(
                List.of(postingsBuilder(FIELD_A, new SideFilePostingsFormatA()), postingsBuilder(FIELD_B, new SideFilePostingsFormatB()))
            ).build(delegate),
            // the manifest is ordered by name, whichever order the plugins were loaded in
            randomSubsetOf(3, List.of(new SideFileDecoratorA(), new SideFileDecoratorB(), new StoredFieldsDecorator()))
        );

        try (Directory directory = newDirectory()) {
            final IndexWriterConfig config = new IndexWriterConfig(null);
            config.setCodec(codec);
            config.setUseCompoundFile(true);
            config.setMergePolicy(newLogMergePolicy(true));

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int i = 0; i < 2; ++i) {
                    final Document document = new Document();
                    document.add(new StringField(FIELD_A, "a" + i, Field.Store.NO));
                    document.add(new StringField(FIELD_B, "b" + i, Field.Store.NO));
                    document.add(new StoredField(STORED_FIELD, "value" + i));
                    writer.addDocument(document);
                    writer.commit();
                }

                // the side files have to survive the merges as well
                writer.forceMerge(1);
            }

            // the codec is resolved by name through the service loader: everything it needs comes from the segment
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(1, reader.leaves().size());

                final SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
                final SegmentInfo si = segmentReader.getSegmentInfo().info;

                assertEquals(Decorated104Codec.NAME, si.getCodec().getName());
                assertNotSame(codec, si.getCodec());
                assertEquals(delegate.getName(), si.getAttribute(DecoratedCodec.DELEGATE_ATTRIBUTE));
                assertEquals(
                    SideFileDecoratorA.NAME + "," + SideFileDecoratorB.NAME + "," + StoredFieldsDecorator.NAME,
                    si.getAttribute(DecoratedCodec.DECORATORS_ATTRIBUTE)
                );

                // the delegate is honored: its stored fields compression mode is the one of the segment
                assertEquals(Lucene104Codec.Mode.BEST_COMPRESSION.name(), si.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY));

                assertTrue(si.getUseCompoundFile());

                // the side files of both plugins are kept outside of the compound file (and are still segment files)
                final Set<String> files = si.files();
                assertEquals("segment files: " + files, 1, sideFiles(files, EXTENSION_A).size());
                assertEquals("segment files: " + files, 1, sideFiles(files, EXTENSION_B).size());
                assertThat(files, hasItem(IndexFileNames.segmentFileName(si.name, "", "cfs")));

                // ... and the postings of both fields are readable, which means the side files were found
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (int i = 0; i < 2; ++i) {
                    assertEquals(1, searcher.count(new TermQuery(new Term(FIELD_A, "a" + i))));
                    assertEquals(1, searcher.count(new TermQuery(new Term(FIELD_B, "b" + i))));
                }

                // the stored fields decorator is applied while reading too, otherwise the prefix it adds would show up
                for (int docId = 0; docId < reader.maxDoc(); ++docId) {
                    final String value = reader.storedFields().document(docId).get(STORED_FIELD);
                    assertThat(value, not(containsString(STORED_FIELDS_PREFIX)));
                    assertTrue("unexpected stored value: " + value, value.startsWith("value"));
                }
            }
        }
    }

    /**
     * The formats of the codec the index is configured with are restored while reading the segment as well:
     * the decorators are applied on top of the ones of the delegate the segment records, not of the default
     * ones (the {@code zstd} codecs of {@code custom-codecs} are the practical example).
     */
    public void testFormatsOfRecordedDelegateAreUsedWhileReading() throws Exception {
        final Codec delegate = new DelegateCodec();
        final Codec codec = DecoratedCodec.wrap(delegate, List.of(new StoredFieldsDecorator()));

        try (Directory directory = newDirectory()) {
            final IndexWriterConfig config = new IndexWriterConfig(null);
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                final Document document = new Document();
                document.add(new StoredField(STORED_FIELD, "value"));
                writer.addDocument(document);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final SegmentInfo si = ((SegmentReader) reader.leaves().get(0).reader()).getSegmentInfo().info;
                assertEquals(DelegateCodec.NAME, si.getAttribute(DecoratedCodec.DELEGATE_ATTRIBUTE));

                // the stored fields format of the delegate rejects the values it did not write itself, so reading
                // only succeeds if both it and the decorator on top of it were restored
                assertEquals("value", reader.storedFields().document(0).get(STORED_FIELD));
            }
        }
    }

    private static Set<String> sideFiles(Collection<String> files, String extension) {
        return files.stream().filter(file -> file.endsWith("." + extension + COMPOUND_SUFFIX)).collect(Collectors.toSet());
    }

    private static SegmentInfo segmentInfo(Directory directory, Codec codec, Map<String, String> attributes) {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            1,
            false,
            false,
            codec,
            Map.of(),
            StringHelper.randomId(),
            attributes,
            null
        );
    }

    private CodecService codecService(List<PerFieldCodecBuilder> builders, List<CodecDecorator> decorators) {
        final Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        return new CodecService(null, indexSettings, LogManager.getLogger("test"), List.of(), builders, decorators);
    }

    private static PerFieldCodecBuilder postingsBuilder(String field, PostingsFormat format) {
        final PerFieldPostingsFormat perField = new PerFieldPostingsFormat() {
            @Override
            public PostingsFormat getPostingsFormatForField(String name) {
                return field.equals(name) ? format : null;
            }
        };

        return new PerFieldCodecBuilder() {
            @Override
            public Optional<PerFieldPostingsFormat> postingsFormat() {
                return Optional.of(perField);
            }
        };
    }

    // ------------------------------------------------------------------------------------------------
    // The plugin doubles below are registered with the service loader (see the "META-INF/services" files
    // of the test resources): the instances created by it are the ones which read the segments back.
    // ------------------------------------------------------------------------------------------------

    /** Keeps the files with the extension of the "plugin" out of the compound file, the way {@code k-NN} does */
    public static class SideFileDecoratorA extends CodecDecorator {
        public static final String NAME = "TestSideFileDecoratorA";

        public SideFileDecoratorA() {
            super(NAME);
        }

        @Override
        public Optional<CompoundFormat> compoundFormat(Codec delegate, CompoundFormat current) {
            return Optional.of(new SideFileCompoundFormat(current, EXTENSION_A));
        }
    }

    /** The same, for another "plugin" owning another extension */
    public static class SideFileDecoratorB extends CodecDecorator {
        public static final String NAME = "TestSideFileDecoratorB";

        public SideFileDecoratorB() {
            super(NAME);
        }

        @Override
        public Optional<CompoundFormat> compoundFormat(Codec delegate, CompoundFormat current) {
            return Optional.of(new SideFileCompoundFormat(current, EXTENSION_B));
        }
    }

    /** Customizes the stored fields, the way the derived source of {@code k-NN} does */
    public static class StoredFieldsDecorator extends CodecDecorator {
        public static final String NAME = "TestStoredFieldsDecorator";

        public StoredFieldsDecorator() {
            super(NAME);
        }

        @Override
        public Optional<StoredFieldsFormat> storedFieldsFormat(Codec delegate) {
            return Optional.of(new PrefixingStoredFieldsFormat(delegate.storedFieldsFormat()));
        }
    }

    /** Only used to verify that the stored fields could not be customized by two decorators */
    public static class SecondStoredFieldsDecorator extends CodecDecorator {
        public static final String NAME = "TestSecondStoredFieldsDecorator";

        public SecondStoredFieldsDecorator() {
            super(NAME);
        }

        @Override
        public Optional<StoredFieldsFormat> storedFieldsFormat(Codec delegate) {
            return Optional.of(delegate.storedFieldsFormat());
        }
    }

    /** Captures the delegate codec its stored fields format reads with */
    public static class DelegateCapturingDecorator extends CodecDecorator {
        public static final String NAME = "TestDelegateCapturingDecorator";
        static final AtomicReference<Codec> READ_WITH = new AtomicReference<>();

        public DelegateCapturingDecorator() {
            super(NAME);
        }

        @Override
        public Optional<StoredFieldsFormat> storedFieldsFormat(Codec delegate) {
            final StoredFieldsFormat format = delegate.storedFieldsFormat();
            return Optional.of(new StoredFieldsFormat() {
                @Override
                public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context)
                    throws IOException {
                    READ_WITH.set(delegate);
                    return format.fieldsReader(directory, si, fn, context);
                }

                @Override
                public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
                    return format.fieldsWriter(directory, si, context);
                }
            });
        }
    }

    /**
     * A codec with a stored fields format of its own (the way the {@code custom-codecs} ones have), registered
     * with the service loader so it could be resolved back while reading the segments which record it.
     */
    public static class DelegateCodec extends FilterCodec {
        public static final String NAME = "TestDecoratedDelegateCodec";
        private static final String PREFIX = "delegate:";

        private final StoredFieldsFormat storedFieldsFormat;

        public DelegateCodec() {
            super(NAME, new Lucene104Codec());
            this.storedFieldsFormat = new PrefixingStoredFieldsFormat(delegate.storedFieldsFormat(), PREFIX, true);
        }

        @Override
        public StoredFieldsFormat storedFieldsFormat() {
            return storedFieldsFormat;
        }
    }

    /** A postings format which writes an additional file, the way the native engines of the plugins do */
    public abstract static class SideFilePostingsFormat extends PostingsFormat {
        private final PostingsFormat delegate = new Lucene104PostingsFormat();
        private final String extension;

        protected SideFilePostingsFormat(String name, String extension) {
            super(name);
            this.extension = extension;
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            final String name = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
            try (IndexOutput output = state.directory.createOutput(name, state.context)) {
                output.writeString(MARKER + extension);
            }
            return delegate.fieldsConsumer(state);
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
            String name = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
            if (state.segmentInfo.getUseCompoundFile()) {
                name += COMPOUND_SUFFIX;
            }

            try (IndexInput input = state.directory.openInput(name, state.context)) {
                final String marker = input.readString();
                if ((MARKER + extension).equals(marker) == false) {
                    throw new IllegalStateException("unexpected content of the side file [" + name + "]: " + marker);
                }
            }

            return delegate.fieldsProducer(state);
        }
    }

    public static class SideFilePostingsFormatA extends SideFilePostingsFormat {
        public SideFilePostingsFormatA() {
            super("TestSideFilePostingsA", EXTENSION_A);
        }
    }

    public static class SideFilePostingsFormatB extends SideFilePostingsFormat {
        public SideFilePostingsFormatB() {
            super("TestSideFilePostingsB", EXTENSION_B);
        }
    }

    /**
     * Copies the files of the "plugin" aside and removes them from the segment before the compound file is
     * written, so they stay outside of it, and serves them from the segment directory while reading.
     */
    private static class SideFileCompoundFormat extends CompoundFormat {
        private final CompoundFormat delegate;
        private final String extension;

        private SideFileCompoundFormat(CompoundFormat delegate, String extension) {
            this.delegate = delegate;
            this.extension = extension;
        }

        @Override
        public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si) throws IOException {
            return new SideFileCompoundDirectory(delegate.getCompoundReader(dir, si), dir, extension + COMPOUND_SUFFIX);
        }

        @Override
        public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
            final Set<String> owned = si.files().stream().filter(file -> file.endsWith("." + extension)).collect(Collectors.toSet());

            if (owned.isEmpty() == false) {
                for (final String file : owned) {
                    dir.copyFrom(dir, file, file + COMPOUND_SUFFIX, context);
                }

                final Set<String> remaining = new HashSet<>(si.files());
                remaining.removeAll(owned);
                si.setFiles(remaining);
            }

            delegate.write(dir, si, context);
        }
    }

    private static class SideFileCompoundDirectory extends CompoundDirectory {
        private final CompoundDirectory delegate;
        private final Directory dir;
        private final String suffix;

        private SideFileCompoundDirectory(CompoundDirectory delegate, Directory dir, String suffix) {
            this.delegate = delegate;
            this.dir = dir;
            this.suffix = suffix;
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public String[] listAll() throws IOException {
            return delegate.listAll();
        }

        @Override
        public long fileLength(String name) throws IOException {
            return name.endsWith(suffix) ? dir.fileLength(name) : delegate.fileLength(name);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return name.endsWith(suffix) ? dir.openInput(name, context) : delegate.openInput(name, context);
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return delegate.getPendingDeletions();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /** Prefixes the stored strings while writing them and strips the prefix back while reading them */
    private static class PrefixingStoredFieldsFormat extends StoredFieldsFormat {
        private final StoredFieldsFormat delegate;
        private final String prefix;
        private final boolean requiredWhileReading;

        private PrefixingStoredFieldsFormat(StoredFieldsFormat delegate) {
            this(delegate, STORED_FIELDS_PREFIX, false);
        }

        private PrefixingStoredFieldsFormat(StoredFieldsFormat delegate, String prefix, boolean requiredWhileReading) {
            this.delegate = delegate;
            this.prefix = prefix;
            this.requiredWhileReading = requiredWhileReading;
        }

        @Override
        public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
            return new PrefixStrippingStoredFieldsReader(delegate.fieldsReader(directory, si, fn, context), prefix, requiredWhileReading);
        }

        @Override
        public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
            return new PrefixingStoredFieldsWriter(delegate.fieldsWriter(directory, si, context), prefix);
        }
    }

    private static class PrefixingStoredFieldsWriter extends StoredFieldsWriter {
        private final StoredFieldsWriter delegate;
        private final String prefix;

        private PrefixingStoredFieldsWriter(StoredFieldsWriter delegate, String prefix) {
            this.delegate = delegate;
            this.prefix = prefix;
        }

        @Override
        public void startDocument() throws IOException {
            delegate.startDocument();
        }

        @Override
        public void finishDocument() throws IOException {
            delegate.finishDocument();
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            delegate.writeField(info, prefix + value);
        }

        @Override
        public void finish(int numDocs) throws IOException {
            delegate.finish(numDocs);
        }

        @Override
        public long ramBytesUsed() {
            return delegate.ramBytesUsed();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class PrefixStrippingStoredFieldsReader extends StoredFieldsReader {
        private final StoredFieldsReader delegate;
        private final String prefix;
        private final boolean required;

        private PrefixStrippingStoredFieldsReader(StoredFieldsReader delegate, String prefix, boolean required) {
            this.delegate = delegate;
            this.prefix = prefix;
            this.required = required;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            delegate.document(docID, new StoredFieldVisitor() {
                @Override
                public Status needsField(FieldInfo fieldInfo) throws IOException {
                    return visitor.needsField(fieldInfo);
                }

                @Override
                public void stringField(FieldInfo fieldInfo, String value) throws IOException {
                    if (value.startsWith(prefix) == false) {
                        if (required) {
                            throw new IllegalStateException("the value [" + value + "] was not written by this format");
                        }
                        visitor.stringField(fieldInfo, value);
                        return;
                    }

                    visitor.stringField(fieldInfo, value.substring(prefix.length()));
                }

                @Override
                public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                    visitor.binaryField(fieldInfo, value);
                }

                @Override
                public void intField(FieldInfo fieldInfo, int value) throws IOException {
                    visitor.intField(fieldInfo, value);
                }

                @Override
                public void longField(FieldInfo fieldInfo, long value) throws IOException {
                    visitor.longField(fieldInfo, value);
                }

                @Override
                public void floatField(FieldInfo fieldInfo, float value) throws IOException {
                    visitor.floatField(fieldInfo, value);
                }

                @Override
                public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
                    visitor.doubleField(fieldInfo, value);
                }
            });
        }

        @Override
        public StoredFieldsReader clone() {
            return new PrefixStrippingStoredFieldsReader(delegate.clone(), prefix, required);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
