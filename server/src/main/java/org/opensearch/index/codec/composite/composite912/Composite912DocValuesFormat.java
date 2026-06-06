/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.composite912;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * DocValues format to handle composite indices
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite912DocValuesFormat extends DocValuesFormat {
    /**
     * Creates a new docvalues format.
     *
     * <p>The provided name will be written into the index segment in some configurations (such as
     * when using {@code PerFieldDocValuesFormat}): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your jar
     * file, etc).
     */
    private final DocValuesFormat delegate;
    private final MapperService mapperService;

    /** Data codec name for Composite Doc Values Format */
    public static final String DATA_CODEC_NAME = "Composite912FormatData";

    /** Meta codec name for Composite Doc Values Format */
    public static final String META_CODEC_NAME = "Composite912FormatMeta";

    /** Filename extension for the composite index data */
    public static final String DATA_EXTENSION = "cid";

    /** Filename extension for the composite index meta */
    public static final String META_EXTENSION = "cim";

    /** Data doc values codec name for Composite Doc Values Format */
    public static final String DATA_DOC_VALUES_CODEC = "Composite912DocValuesData";

    /** Meta doc values codec name for Composite Doc Values Format */
    public static final String META_DOC_VALUES_CODEC = "Composite912DocValuesMetadata";

    /** Filename extension for the composite index data doc values */
    public static final String DATA_DOC_VALUES_EXTENSION = "cidvd";

    /** Filename extension for the composite index meta doc values */
    public static final String META_DOC_VALUES_EXTENSION = "cidvm";

    /** Initial version for the Composite90DocValuesFormat */
    public static final int VERSION_START = 0;

    /** Current version for the Composite90DocValuesFormat */
    public static final int VERSION_CURRENT = VERSION_START;

    // needed for SPI
    public Composite912DocValuesFormat() {
        this(new Lucene90DocValuesFormat(), null);
    }

    public Composite912DocValuesFormat(MapperService mapperService) {
        this(new Lucene90DocValuesFormat(), mapperService);
    }

    public Composite912DocValuesFormat(DocValuesFormat delegate, MapperService mapperService) {
        super(delegate.getName());
        this.delegate = delegate;
        this.mapperService = mapperService;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new Composite912DocValuesWriter(delegate.fieldsConsumer(state), state, mapperService);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        DocValuesProducer regularProducer;

        // Check if this segment was retroactively upgraded from a PerField codec. If so,
        // doc values use per-field naming (e.g., _0_Lucene90_0.dvd) instead of direct naming.
        //
        // Verify the suffixed file exists before using it — merged segments may inherit
        // stale PerFieldDocValuesFormat attributes but write with empty suffix.
        String perFieldSuffix = getPerFieldDocValuesSuffix(state.fieldInfos);
        if (perFieldSuffix != null && perFieldSuffixedFileExists(state, perFieldSuffix)) {
            // For upgraded segments, read doc values with the original per-field suffix.
            // state.fieldInfos may have renumbered fields due to soft delete updates.
            SegmentReadState suffixedState = new SegmentReadState(
                state.directory,
                state.segmentInfo,
                state.fieldInfos,
                state.context,
                perFieldSuffix
            );
            regularProducer = delegate.fieldsProducer(suffixedState);
        } else {
            // Native Composite912 segment (or merged segment): use direct naming
            regularProducer = delegate.fieldsProducer(state);
        }

        return new Composite912DocValuesReader(regularProducer, state);
    }

    /**
     * Checks if the segment's FieldInfos contain PerFieldDocValuesFormat attributes,
     * indicating the segment was originally written by a PerField codec (e.g., Lucene912Codec).
     * Returns the full suffix string (e.g., "Lucene90_0") needed to open the doc values files,
     * or null if this is a native Composite912 segment.
     */
    static String getPerFieldDocValuesSuffix(FieldInfos fieldInfos) {
        for (FieldInfo fi : fieldInfos) {
            String formatName = fi.getAttribute("PerFieldDocValuesFormat.format");
            String suffix = fi.getAttribute("PerFieldDocValuesFormat.suffix");
            if (formatName != null && suffix != null) {
                return formatName + "_" + suffix;
            }
        }
        return null;
    }

    /**
     * Checks if the per-field suffixed doc values metadata file actually exists on disk.
     * Merged segments produced by Composite912Codec write doc values with empty suffix,
     * but their FieldInfos may still carry PerFieldDocValuesFormat attributes inherited
     * from source segments. This method prevents using a stale suffix that would cause
     * NoSuchFileException when opening the merged segment.
     */
    private static boolean perFieldSuffixedFileExists(SegmentReadState state, String perFieldSuffix) {
        String suffixedMetaFile = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            perFieldSuffix,
            "dvm"
        );
        try {
            state.directory.openInput(suffixedMetaFile, state.context).close();
            return true;
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            return false;
        } catch (java.io.IOException e) {
            // If we can't determine, assume it doesn't exist and fall back to empty suffix
            return false;
        }
    }
}
