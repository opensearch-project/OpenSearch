/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * A {@link WriterFileSet} subclass that guarantees exactly one file per writer generation.
 * <p>
 * Data formats that produce a single file per segment (e.g., Parquet — one {@code .parquet}
 * file per writer generation) should use this class to make the single-file invariant
 * explicit at the type level. This eliminates a class of bugs where downstream consumers
 * (e.g., native readers that map one file = one segment) silently break if multiple files
 * are present.
 * <p>
 * Since this extends {@link WriterFileSet}, it is substitutable anywhere the base class
 * is accepted — no conversion needed. Use {@link #file()} for direct single-file access.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class MonoFileWriterSet extends WriterFileSet {

    private final String file;

    private MonoFileWriterSet(String directory, long writerGeneration, String file, long numRows, long formatVersion) {
        super(directory, writerGeneration, Set.of(file), numRows, formatVersion);
        this.file = file;
    }

    /**
     * Creates a MonoFileWriterSet from explicit values.
     *
     * @param directory        the directory containing the file
     * @param writerGeneration the writer generation that produced this file
     * @param file             the single file name
     * @param numRows          the number of rows in the file
     */
    public static MonoFileWriterSet of(String directory, long writerGeneration, String file, long numRows) {
        return of(directory, writerGeneration, file, numRows, 0L);
    }

    /**
     * Creates a MonoFileWriterSet from explicit values, including a data-format version.
     *
     * @param directory        the directory containing the file
     * @param writerGeneration the writer generation that produced this file
     * @param file             the single file name
     * @param numRows          the number of rows in the file
     * @param formatVersion    the long-encoded data-format version stamp; {@code 0L} means
     *                         "unknown / pre-versioning" (see {@code LuceneVersionConverter})
     */
    public static MonoFileWriterSet of(String directory, long writerGeneration, String file, long numRows, long formatVersion) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("file must not be null or empty");
        }
        return new MonoFileWriterSet(directory, writerGeneration, file, numRows, formatVersion);
    }

    /**
     * Creates a MonoFileWriterSet from a directory path, generation, file name, and row count.
     */
    public static MonoFileWriterSet of(Path directory, long writerGeneration, String file, long numRows) {
        return of(directory.toAbsolutePath().toString(), writerGeneration, file, numRows, 0L);
    }

    /**
     * Creates a MonoFileWriterSet from a directory path, generation, file name, row count, and format version.
     */
    public static MonoFileWriterSet of(Path directory, long writerGeneration, String file, long numRows, long formatVersion) {
        return of(directory.toAbsolutePath().toString(), writerGeneration, file, numRows, formatVersion);
    }

    /**
     * Narrows a {@link WriterFileSet} to a {@link MonoFileWriterSet}, validating that it
     * contains exactly one file.
     *
     * @param wfs the writer file set to narrow
     * @throws IllegalArgumentException if the file set does not contain exactly one file
     */
    public static MonoFileWriterSet from(WriterFileSet wfs) {
        if (wfs instanceof MonoFileWriterSet mono) {
            return mono;
        }
        if (wfs.files().size() != 1) {
            throw new IllegalArgumentException(
                "MonoFileWriterSet requires exactly one file per generation, but generation "
                    + wfs.writerGeneration()
                    + " has "
                    + wfs.files().size()
                    + " files: "
                    + wfs.files()
            );
        }
        return new MonoFileWriterSet(
            wfs.directory(),
            wfs.writerGeneration(),
            wfs.files().iterator().next(),
            wfs.numRows(),
            wfs.formatVersion()
        );
    }

    /**
     * Deserializes a MonoFileWriterSet from a stream.
     */
    public MonoFileWriterSet(StreamInput in, String directory, long version) throws IOException {
        this(directory, in.readLong(), in.readString(), in.readLong(), version);
    }

    /**
     * Returns the single file in this set.
     */
    public String file() {
        return file;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(writerGeneration());
        out.writeStringCollection(files());
        out.writeLong(numRows());
        out.writeLong(formatVersion());
    }

    @Override
    public String toString() {
        return "MonoFileWriterSet{directory="
            + directory()
            + ", writerGeneration="
            + writerGeneration()
            + ", file="
            + file
            + ", formatVersion="
            + formatVersion()
            + '}';
    }
}
