/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The {@link Codec} which applies the {@link CodecDecorator}s contributed by the plugins on top of the
 * {@link Codec} the index is configured with. It is only used when at least one plugin contributes a
 * decorator, otherwise the index codec (with the per-field formats injected, see please
 * {@link CompositeDelegatingPerFieldCodec}) is used as-is.
 *
 * <p>
 * Since the per-segment formats are only reachable through the codec of the segment, and a segment
 * records the name of its codec, this codec is self-describing: while writing a segment it records the
 * name of the delegate codec and the names of the decorators applied, so that
 * {@link #segmentInfoFormat()} aside, every format could be reconstructed exactly while reading the
 * segment (the instance the service loader creates has no other context).
 *
 * <p>
 * The {@link SegmentInfoFormat} is the one exception: it is the format which reads the very attributes
 * the reconstruction is based upon, so it is pinned by the codec version (this is why the codec is
 * versioned after the Lucene codec generation it is compatible with, exactly like the codecs of Apache
 * Lucene itself). On a Lucene upgrade a new version has to be introduced and the existing ones have to
 * be kept (see please {@link Decorated104Codec}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class DecoratedCodec extends Codec {
    /** All the versions of this codec share the prefix, so they could be told apart from the index codecs */
    static final String NAME_PREFIX = "OpenSearchDecorated";
    /** The {@link SegmentInfo} attribute which holds the name of the delegate {@link Codec} */
    static final String DELEGATE_ATTRIBUTE = "opensearch.codec.delegate";
    /** The {@link SegmentInfo} attribute which holds the names of the applied {@link CodecDecorator}s */
    static final String DECORATORS_ATTRIBUTE = "opensearch.codec.decorators";

    private static final String DECORATORS_SEPARATOR = ",";

    /**
     * Wraps the provided {@link Codec} into the current version of the {@link DecoratedCodec}, or
     * returns it unchanged if there are no decorators to apply.
     */
    public static Codec wrap(Codec delegate, List<CodecDecorator> decorators) {
        if (decorators.isEmpty()) {
            return delegate;
        }

        // sorted, so the manifest does not depend on the order the plugins were loaded in
        final List<CodecDecorator> sorted = decorators.stream().sorted(Comparator.comparing(CodecDecorator::getName)).toList();
        for (int i = 1; i < sorted.size(); ++i) {
            if (sorted.get(i - 1).getName().equals(sorted.get(i).getName())) {
                throw new IllegalStateException("The codec decorator [" + sorted.get(i).getName() + "] is contributed more than once");
            }
        }

        return new Decorated104Codec(delegate, sorted);
    }

    /**
     * Returns {@code true} if the provided codec name belongs to one of the {@link DecoratedCodec}
     * versions: such codecs are an implementation detail and could not be requested by name (they only
     * exist to read back what they wrote).
     */
    public static boolean isDecoratedCodec(String name) {
        return name != null && name.startsWith(NAME_PREFIX);
    }

    /** The formats resolved for a particular delegate codec and chain of decorators */
    private static final class Formats {
        private final Codec delegate;
        private final CompoundFormat compoundFormat;
        private final StoredFieldsFormat storedFieldsFormat;
        private final String delegateName;
        private final String decoratorNames;

        private Formats(Codec delegate, List<CodecDecorator> decorators) {
            this.delegate = delegate;
            this.delegateName = delegate.getName();
            this.decoratorNames = decorators.stream().map(CodecDecorator::getName).collect(Collectors.joining(DECORATORS_SEPARATOR));

            CompoundFormat compound = delegate.compoundFormat();
            StoredFieldsFormat storedFields = delegate.storedFieldsFormat();
            String storedFieldsDecorator = null;

            for (final CodecDecorator decorator : decorators) {
                compound = decorator.compoundFormat(delegate, compound).orElse(compound);

                final Optional<StoredFieldsFormat> decorated = decorator.storedFieldsFormat(delegate);
                if (decorated.isPresent()) {
                    if (storedFieldsDecorator != null) {
                        throw new IllegalStateException(
                            "The stored fields format is already customized by ["
                                + storedFieldsDecorator
                                + "], it could not be customized again by ["
                                + decorator.getName()
                                + "]"
                        );
                    }

                    storedFields = decorated.get();
                    storedFieldsDecorator = decorator.getName();
                }
            }

            this.compoundFormat = compound;
            this.storedFieldsFormat = storedFields;
        }
    }

    /**
     * The formats to write with, "null" for the instances created by the service loader. The write instance
     * hands out the delegate's own format instances rather than the resolving wrappers below, so whatever
     * wraps this codec and inspects its formats (the {@link CriteriaBasedCodec}, for example, which only
     * customizes a {@link org.apache.lucene.codecs.perfield.PerFieldPostingsFormat}) keeps working.
     */
    @Nullable
    private final Formats writeFormats;
    /** The formats to read with, resolved from the segment attributes and cached per combination */
    private final Map<String, Formats> readFormats = new ConcurrentHashMap<>();
    /** The format which reads and writes the segment attributes the reconstruction is based upon */
    private final SegmentInfoFormat versionedSegmentInfoFormat;
    private final Codec versionedCodec;

    private final PostingsFormat postingsFormat;
    private final DocValuesFormat docValuesFormat;
    private final KnnVectorsFormat knnVectorsFormat;
    private final StoredFieldsFormat storedFieldsFormat;
    private final TermVectorsFormat termVectorsFormat;
    private final FieldInfosFormat fieldInfosFormat;
    private final SegmentInfoFormat segmentInfoFormat;
    private final NormsFormat normsFormat;
    private final LiveDocsFormat liveDocsFormat;
    private final CompoundFormat compoundFormat;
    private final PointsFormat pointsFormat;

    /**
     * Creates the codec used to read the segments: the delegate and the decorators are resolved from
     * every segment being read.
     *
     * @param name the (versioned) name of this codec
     * @param versionedCodec the Lucene codec generation this version is compatible with
     */
    protected DecoratedCodec(String name, Codec versionedCodec) {
        this(name, versionedCodec, null);
    }

    /**
     * Creates the codec used to write the segments with the provided delegate and decorators.
     *
     * @param name the (versioned) name of this codec
     * @param versionedCodec the Lucene codec generation this version is compatible with
     * @param delegate the codec of the index
     * @param decorators the decorators contributed by the plugins, in the order they are applied
     */
    protected DecoratedCodec(String name, Codec versionedCodec, Codec delegate, List<CodecDecorator> decorators) {
        this(name, versionedCodec, new Formats(Objects.requireNonNull(delegate), List.copyOf(decorators)));

        if (delegate.segmentInfoFormat().getClass() != versionedSegmentInfoFormat.getClass()) {
            throw new IllegalStateException(
                "The codec ["
                    + delegate.getName()
                    + "] could not be decorated: it uses the segment info format ["
                    + delegate.segmentInfoFormat().getClass().getName()
                    + "] while ["
                    + name
                    + "] is only compatible with ["
                    + versionedSegmentInfoFormat.getClass().getName()
                    + "]"
            );
        }

        if (isDecoratedCodec(delegate.getName())) {
            throw new IllegalStateException("The codec [" + delegate.getName() + "] is already decorated");
        }
    }

    private DecoratedCodec(String name, Codec versionedCodec, @Nullable Formats writeFormats) {
        super(name);

        assert name.startsWith(NAME_PREFIX) : "the name of a decorated codec must start with " + NAME_PREFIX;

        this.versionedCodec = Objects.requireNonNull(versionedCodec);
        this.versionedSegmentInfoFormat = versionedCodec.segmentInfoFormat();
        this.writeFormats = writeFormats;

        this.postingsFormat = new PostingsFormat(name) {
            @Override
            public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                return formats(state.segmentInfo).delegate.postingsFormat().fieldsConsumer(state);
            }

            @Override
            public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
                return formats(state.segmentInfo).delegate.postingsFormat().fieldsProducer(state);
            }
        };

        this.docValuesFormat = new DocValuesFormat(name) {
            @Override
            public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                return formats(state.segmentInfo).delegate.docValuesFormat().fieldsConsumer(state);
            }

            @Override
            public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
                return formats(state.segmentInfo).delegate.docValuesFormat().fieldsProducer(state);
            }
        };

        this.knnVectorsFormat = new KnnVectorsFormat(name) {
            @Override
            public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                return formats(state.segmentInfo).delegate.knnVectorsFormat().fieldsWriter(state);
            }

            @Override
            public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                return formats(state.segmentInfo).delegate.knnVectorsFormat().fieldsReader(state);
            }

            @Override
            public int getMaxDimensions(String fieldName) {
                // only consulted while indexing, which the write instance serves with the delegate's own format
                return versionedCodec.knnVectorsFormat().getMaxDimensions(fieldName);
            }
        };

        this.storedFieldsFormat = new StoredFieldsFormat() {
            @Override
            public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context)
                throws IOException {
                return formats(si).storedFieldsFormat.fieldsReader(directory, si, fn, context);
            }

            @Override
            public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
                return formats(si).storedFieldsFormat.fieldsWriter(directory, si, context);
            }
        };

        this.termVectorsFormat = new TermVectorsFormat() {
            @Override
            public TermVectorsReader vectorsReader(Directory directory, SegmentInfo si, FieldInfos fieldInfos, IOContext context)
                throws IOException {
                return formats(si).delegate.termVectorsFormat().vectorsReader(directory, si, fieldInfos, context);
            }

            @Override
            public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
                return formats(si).delegate.termVectorsFormat().vectorsWriter(directory, si, context);
            }
        };

        this.fieldInfosFormat = new FieldInfosFormat() {
            @Override
            public FieldInfos read(Directory directory, SegmentInfo si, String segmentSuffix, IOContext context) throws IOException {
                return formats(si).delegate.fieldInfosFormat().read(directory, si, segmentSuffix, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo si, String segmentSuffix, FieldInfos infos, IOContext context)
                throws IOException {
                formats(si).delegate.fieldInfosFormat().write(directory, si, segmentSuffix, infos, context);
            }
        };

        this.segmentInfoFormat = new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return versionedSegmentInfoFormat.read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
                if (writeFormats != null) {
                    // the segment has to carry everything needed to reconstruct the formats while reading it
                    info.putAttribute(DELEGATE_ATTRIBUTE, writeFormats.delegateName);
                    info.putAttribute(DECORATORS_ATTRIBUTE, writeFormats.decoratorNames);
                }

                versionedSegmentInfoFormat.write(dir, info, ioContext);
            }
        };

        this.normsFormat = new NormsFormat() {
            @Override
            public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
                return formats(state.segmentInfo).delegate.normsFormat().normsConsumer(state);
            }

            @Override
            public NormsProducer normsProducer(SegmentReadState state) throws IOException {
                return formats(state.segmentInfo).delegate.normsFormat().normsProducer(state);
            }
        };

        this.liveDocsFormat = new LiveDocsFormat() {
            @Override
            public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException {
                return formats(info.info).delegate.liveDocsFormat().readLiveDocs(dir, info, context);
            }

            @Override
            public void writeLiveDocs(Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context)
                throws IOException {
                formats(info.info).delegate.liveDocsFormat().writeLiveDocs(bits, dir, info, newDelCount, context);
            }

            @Override
            public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
                formats(info.info).delegate.liveDocsFormat().files(info, files);
            }
        };

        this.compoundFormat = new CompoundFormat() {
            @Override
            public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si) throws IOException {
                return formats(si).compoundFormat.getCompoundReader(dir, si);
            }

            @Override
            public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
                formats(si).compoundFormat.write(dir, si, context);
            }
        };

        this.pointsFormat = new PointsFormat() {
            @Override
            public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                return formats(state.segmentInfo).delegate.pointsFormat().fieldsWriter(state);
            }

            @Override
            public PointsReader fieldsReader(SegmentReadState state) throws IOException {
                return formats(state.segmentInfo).delegate.pointsFormat().fieldsReader(state);
            }
        };
    }

    /**
     * Resolves the formats of the provided segment: the ones this codec was created with, if it is used
     * to write (only the segment info format reaches here then), or the ones recorded in the segment itself.
     */
    private Formats formats(SegmentInfo si) {
        if (writeFormats != null) {
            return writeFormats;
        }

        final String delegateName = si.getAttribute(DELEGATE_ATTRIBUTE);
        if (delegateName == null) {
            throw new IllegalStateException(
                "The segment ["
                    + si.name
                    + "] is written with ["
                    + getName()
                    + "] but does not record the delegate codec, the index is either corrupted or was written by a newer version"
            );
        }

        final String decoratorNames = (si.getAttribute(DECORATORS_ATTRIBUTE) == null) ? "" : si.getAttribute(DECORATORS_ATTRIBUTE);
        final String key = delegateName + "|" + decoratorNames;

        final Formats cached = readFormats.get(key);
        if (cached != null) {
            return cached;
        }

        final Formats resolved = new Formats(Codec.forName(delegateName), decorators(decoratorNames, si));
        final Formats existing = readFormats.putIfAbsent(key, resolved);
        return (existing != null) ? existing : resolved;
    }

    private static List<CodecDecorator> decorators(String names, SegmentInfo si) {
        if (names.isEmpty()) {
            return List.of();
        }

        final List<CodecDecorator> decorators = new ArrayList<>();
        for (final String name : names.split(DECORATORS_SEPARATOR)) {
            try {
                decorators.add(CodecDecorator.forName(name));
            } catch (final IllegalArgumentException ex) {
                throw new IllegalStateException(
                    "The segment ["
                        + si.name
                        + "] was written with the codec decorator ["
                        + name
                        + "] which is not available, the plugin which contributed it is likely not installed anymore",
                    ex
                );
            }
        }

        return decorators;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return (writeFormats != null) ? writeFormats.delegate.postingsFormat() : postingsFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return (writeFormats != null) ? writeFormats.delegate.docValuesFormat() : docValuesFormat;
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return (writeFormats != null) ? writeFormats.delegate.knnVectorsFormat() : knnVectorsFormat;
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return (writeFormats != null) ? writeFormats.storedFieldsFormat : storedFieldsFormat;
    }

    @Override
    public final TermVectorsFormat termVectorsFormat() {
        return (writeFormats != null) ? writeFormats.delegate.termVectorsFormat() : termVectorsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return (writeFormats != null) ? writeFormats.delegate.fieldInfosFormat() : fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return segmentInfoFormat;
    }

    @Override
    public final NormsFormat normsFormat() {
        return (writeFormats != null) ? writeFormats.delegate.normsFormat() : normsFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return (writeFormats != null) ? writeFormats.delegate.liveDocsFormat() : liveDocsFormat;
    }

    @Override
    public final CompoundFormat compoundFormat() {
        return (writeFormats != null) ? writeFormats.compoundFormat : compoundFormat;
    }

    @Override
    public final PointsFormat pointsFormat() {
        return (writeFormats != null) ? writeFormats.delegate.pointsFormat() : pointsFormat;
    }
}
