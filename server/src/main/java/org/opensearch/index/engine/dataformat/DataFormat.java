/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a data format for storing and managing index data, with declared capabilities.
 * Each data format (e.g., Lucene, Parquet) declares what storage and query capabilities it supports.
 * <p>
 * Equality is based on the format {@link #name()} — there should be one {@code DataFormat} instance
 * per unique name. This allows {@code DataFormat} to be used safely as a {@link java.util.Map} key.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class DataFormat {

    /**
     * Name prefix reserved for <em>auxiliary</em> data formats — formats that back a side table
     * rather than the shard's documents. The nested child table (one row per nested element) is
     * the first such use.
     *
     * <p>The marker lives in the name rather than in a field on {@link
     * org.opensearch.index.engine.exec.Segment} for two reasons: {@code Segment} is
     * {@link org.opensearch.core.common.io.stream.Writeable} and carries only format <em>names</em>,
     * so a name-based test needs no wire-format change and works on a deserialised segment; and the
     * two places that derive a shard's document count from segments are {@code static} helpers with
     * no access to a {@link DataFormatRegistry}.
     */
    public static final String AUXILIARY_NAME_PREFIX = "aux__";

    /**
     * Returns whether {@code formatName} denotes an auxiliary (side-table) format, whose rows are
     * <em>not</em> the shard's documents and must therefore be excluded from document counts.
     *
     * @param formatName a data format name, possibly null
     * @return true when the name carries {@link #AUXILIARY_NAME_PREFIX}
     */
    public static boolean isAuxiliaryFormatName(String formatName) {
        return formatName != null && formatName.startsWith(AUXILIARY_NAME_PREFIX);
    }

    /**
     * Name-only counterpart of {@link #storageName()}: maps a format name to the on-disk layout its
     * files live in, for the callers that hold a name rather than a {@link DataFormat}.
     *
     * <p>{@code Store#shardFormatDirectoryResolver} is the reason this exists. A serialised
     * {@link org.opensearch.index.engine.exec.Segment} carries format names, so recovery and
     * replication rebuild a catalog from names alone and must still resolve an auxiliary format to
     * its delegate's directory — the one its files were actually written to.
     *
     * @param formatName a data format name, possibly null
     * @return the delegate's name for an auxiliary format, otherwise {@code formatName} unchanged
     */
    public static String storageNameOf(String formatName) {
        if (isAuxiliaryFormatName(formatName) == false) {
            return formatName;
        }
        String withoutPrefix = formatName.substring(AUXILIARY_NAME_PREFIX.length());
        int roleAt = withoutPrefix.lastIndexOf(AuxiliaryDataFormat.ROLE_SEPARATOR);
        if (roleAt <= 0) {
            // Not a name this class produced. Returning it verbatim keeps the caller's behaviour
            // unchanged rather than inventing a directory from a malformed name.
            return formatName;
        }
        return withoutPrefix.substring(0, roleAt);
    }

    /**
     * Returns the unique name of this data format.
     *
     * @return the data format name
     */
    public abstract String name();

    /**
     * Returns whether this format backs a side table rather than the shard's documents.
     * See {@link #AUXILIARY_NAME_PREFIX}.
     *
     * @return true for auxiliary formats
     */
    public final boolean isAuxiliary() {
        return isAuxiliaryFormatName(name());
    }

    /**
     * Returns the format that owns the <em>physical</em> storage this format's files live in, which
     * is <em>not</em> always {@code this}.
     *
     * <p>{@link #name()} is a <em>logical</em> identity: it keys the catalog, so
     * {@code Segment#dfGroupedSearchableFiles} and
     * {@code CatalogSnapshot#getSearchableFiles(String)} must always be addressed with it. This
     * method is the <em>physical</em> identity — whose subdirectory of the shard data path the files
     * sit in, and correspondingly whose store resources (native store handle, checksum strategy,
     * {@code FormatStore}) apply to them.
     *
     * <p>The two coincide for every document format, and diverge for an
     * {@linkplain AuxiliaryDataFormat auxiliary} one: a side table is written by its delegate
     * format's own writer, so its files land in the delegate's directory, distinguished from the
     * parent's only by a generation-derived file name. Conflating the two would send a child reader
     * looking in a directory that does not exist; giving the side table storage resources of its
     * <em>own</em> is worse still, because nothing on the write or recovery path would ever populate
     * them — the files are, physically, the delegate's.
     *
     * @return the format whose storage backs this one's files; {@code this} by default
     */
    public DataFormat storageFormat() {
        return this;
    }

    /**
     * Returns the name of the on-disk layout this format's files live in — the per-format
     * subdirectory of the shard data path that readers resolve files against. See
     * {@link #storageFormat()}, which is the single point of override.
     *
     * @return the directory name this format's files are stored under
     */
    public final String storageName() {
        return storageFormat().name();
    }

    /**
     * Returns the priority of this data format. Higher priority formats are preferred
     * when multiple formats can handle the same field type.
     *
     * @return the priority value
     */
    public abstract long priority();

    /**
     * Returns the set of field type capabilities supported by this data format.
     *
     * @return the supported field type capabilities
     */
    public abstract Set<FieldTypeCapabilities> supportedFields();

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DataFormat == false) return false;
        return Objects.equals(name(), ((DataFormat) o).name());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(name());
    }
}
