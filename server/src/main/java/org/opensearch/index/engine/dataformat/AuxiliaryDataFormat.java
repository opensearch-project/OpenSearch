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
 * A {@link DataFormat} naming a <em>side table</em> written in the same underlying format as the
 * shard's documents, but whose rows are not documents.
 *
 * <p>The nested child table is the motivating case: a {@code nested} field is stored as one row
 * per element in a separate table, linked back to its parent row by a foreign key. Those rows live
 * in parquet and lucene files exactly like the parent's, so the side table needs no new wire
 * format, no new indexing engine, and no new reader implementation — it needs a distinct
 * <em>name</em>, so that the catalog can hold its files separately (see
 * {@code CatalogSnapshot#getSearchableFiles(String)}) and a reader manager can be opened over just
 * those files.
 *
 * <p>An auxiliary format therefore delegates every declaration to the format it sits beside
 * ({@link #priority()}, {@link #supportedFields()}) and overrides only {@link #name()}, which
 * carries {@link DataFormat#AUXILIARY_NAME_PREFIX} so that document counts derived from segments
 * exclude its rows. See {@code Segment#isAuxiliaryOnly}.
 *
 * <p>Auxiliary formats are registered via {@link DataFormatPlugin#getAuxiliaryDataFormats()}. They
 * are resolvable by name and may be given a reader manager, but they are <em>not</em> entered in
 * the registry's format-to-plugin map: a side table has no indexing engine of its own, because its
 * writers are created from the engine of the format it delegates to.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class AuxiliaryDataFormat extends DataFormat {

    /** Separates the delegate format name from the role in an auxiliary format name. */
    public static final String ROLE_SEPARATOR = "__";

    /**
     * Role of the child table holding one row per element of a {@code nested} field.
     *
     * <p>This constant is here, rather than in the composite engine that owns the nested design,
     * because the format name has to be spelled identically by four packages that do not depend on
     * one another: the format owners ({@code parquet-data-format},
     * {@code analytics-backend-lucene}), the read backends ({@code analytics-backend-datafusion}
     * and again {@code analytics-backend-lucene}), and the composite engine that writes the table.
     * {@code server} is their only common ancestor.
     */
    public static final String NESTED_CHILD_ROLE = "nested";

    /**
     * Added to a writer generation to derive the generation of the side table that writer produced.
     *
     * <p>A side table is published as its own {@link org.opensearch.index.engine.exec.Segment}, and a
     * segment is identified by its generation — so the side table cannot reuse the generation of the
     * refresh that produced it, or the two would collide in the catalog and in every generation-keyed
     * file name. Offsetting instead of allocating a fresh generation is what makes the pairing
     * <em>derivable</em>: {@link #generationFor} and {@link #writerGenerationOf} are inverses, so any
     * component holding one segment can compute the other without a side map. The merge path relies
     * on exactly that to keep a side table in lockstep with the documents it describes — see
     * {@code MergeHandler#findMerges}.
     *
     * <p>2<sup>40</sup> is chosen so the two ranges cannot meet in practice: a shard would need a
     * trillion refreshes to reach the offset from below. It is also wide enough to be obvious in a
     * file name, since generations are rendered in hex — {@code _parquet_file_generation_10000000001}
     * is 2<sup>40</sup>+1, visibly a side table of generation 1.
     */
    public static final long GENERATION_OFFSET = 1L << 40;

    /**
     * Returns the generation of the side table produced by the writer at {@code writerGeneration}.
     *
     * @param writerGeneration generation of the refresh that produced the side table
     * @return the side table's segment generation
     * @throws IllegalArgumentException if {@code writerGeneration} is not a document generation
     */
    public static long generationFor(long writerGeneration) {
        if (writerGeneration <= 0 || isAuxiliaryGeneration(writerGeneration)) {
            throw new IllegalArgumentException(
                "Writer generation must be a positive document generation below the auxiliary offset ["
                    + GENERATION_OFFSET
                    + "] but was ["
                    + writerGeneration
                    + "]"
            );
        }
        return writerGeneration + GENERATION_OFFSET;
    }

    /**
     * Returns the writer generation that produced the side table at {@code auxiliaryGeneration} —
     * the inverse of {@link #generationFor}.
     *
     * @param auxiliaryGeneration a side table's segment generation
     * @return the generation of the refresh that produced it
     * @throws IllegalArgumentException if {@code auxiliaryGeneration} is not an auxiliary generation
     */
    public static long writerGenerationOf(long auxiliaryGeneration) {
        if (isAuxiliaryGeneration(auxiliaryGeneration) == false) {
            throw new IllegalArgumentException(
                "Not an auxiliary generation: [" + auxiliaryGeneration + "] is below the offset [" + GENERATION_OFFSET + "]"
            );
        }
        return auxiliaryGeneration - GENERATION_OFFSET;
    }

    /** Returns whether {@code generation} is in the auxiliary range, i.e. names a side table. */
    public static boolean isAuxiliaryGeneration(long generation) {
        return generation > GENERATION_OFFSET;
    }

    private final DataFormat delegate;
    private final String role;
    private final String name;

    /**
     * Creates an auxiliary format sitting beside {@code delegate}.
     *
     * @param delegate the format whose files, engine and reader the side table reuses
     * @param role     what the side table holds, e.g. {@link #NESTED_CHILD_ROLE}
     * @throws IllegalArgumentException if {@code delegate} is itself auxiliary, or {@code role} is
     *                                  empty or contains {@link #ROLE_SEPARATOR}
     */
    public AuxiliaryDataFormat(DataFormat delegate, String role) {
        Objects.requireNonNull(delegate, "delegate data format is required");
        Objects.requireNonNull(role, "role is required");
        if (delegate.isAuxiliary()) {
            throw new IllegalArgumentException("Cannot nest auxiliary formats: delegate [" + delegate.name() + "] is already auxiliary");
        }
        if (role.isEmpty() || role.contains(ROLE_SEPARATOR)) {
            throw new IllegalArgumentException(
                "Auxiliary role [" + role + "] must be non-empty and must not contain [" + ROLE_SEPARATOR + "]"
            );
        }
        this.delegate = delegate;
        this.role = role;
        this.name = nameFor(delegate.name(), role);
    }

    /**
     * Returns the name an auxiliary format takes for the given delegate and role, without needing
     * the delegate's {@link DataFormat} instance. Read backends use this: {@code
     * analytics-backend-datafusion} declares support for parquet by name only and has no compile
     * dependency on {@code parquet-data-format}.
     *
     * @param delegateFormatName the delegate format's {@link DataFormat#name()}
     * @param role               the auxiliary role
     * @return the auxiliary format name
     */
    public static String nameFor(String delegateFormatName, String role) {
        return AUXILIARY_NAME_PREFIX + delegateFormatName + ROLE_SEPARATOR + role;
    }

    /**
     * Returns the role encoded in an auxiliary format name — the counterpart of
     * {@link DataFormat#storageNameOf}, which returns the other half.
     *
     * @param formatName an auxiliary format name as produced by {@link #nameFor}
     * @return the role, or {@code null} if {@code formatName} is not an auxiliary name this class
     *         could have produced
     */
    public static String roleOf(String formatName) {
        if (DataFormat.isAuxiliaryFormatName(formatName) == false) {
            return null;
        }
        String withoutPrefix = formatName.substring(AUXILIARY_NAME_PREFIX.length());
        int roleAt = withoutPrefix.lastIndexOf(ROLE_SEPARATOR);
        if (roleAt <= 0 || roleAt + ROLE_SEPARATOR.length() >= withoutPrefix.length()) {
            return null;
        }
        return withoutPrefix.substring(roleAt + ROLE_SEPARATOR.length());
    }

    /** Returns the format whose files, indexing engine and reader this side table reuses. */
    public DataFormat delegate() {
        return delegate;
    }

    /** Returns what this side table holds, e.g. {@link #NESTED_CHILD_ROLE}. */
    public String role() {
        return role;
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * {@inheritDoc} Delegated: the side table's files are produced by the delegate format's own
     * writer, so they sit in the delegate's directory and are told apart from the parent's by their
     * generation-derived file name. Only the catalog key differs — and with it every store resource
     * keyed by format, which must resolve to the delegate's rather than to one of this format's own.
     */
    @Override
    public DataFormat storageFormat() {
        return delegate;
    }

    /** {@inheritDoc} Delegated — a side table is written by the delegate's engine. */
    @Override
    public long priority() {
        return delegate.priority();
    }

    /** {@inheritDoc} Delegated — a side table stores the same field types as its delegate. */
    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return delegate.supportedFields();
    }
}
