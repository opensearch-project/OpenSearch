/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.nativebridge.spi.ArrowExport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Managed wrapper around an Apache Arrow {@link VectorSchemaRoot} with strict lifecycle enforcement.
 *
 * <p>Each instance follows the state machine: {@code ACTIVE → FROZEN → CLOSED}.
 * <ul>
 *   <li><strong>ACTIVE</strong> — Vectors are writable; row count can be incremented.</li>
 *   <li><strong>FROZEN</strong> — Read-only; data can be exported to the native writer via
 *       {@link #exportToArrow()} using the Arrow C Data Interface.</li>
 *   <li><strong>CLOSED</strong> — All Arrow resources (vectors and child allocator) are released.</li>
 * </ul>
 *
 * <p>State transitions are enforced: writing to a frozen VSR or closing an active VSR
 * (without freezing first) throws {@link IllegalStateException}.
 *
 * <p>This class is NOT Thread-Safe. External synchronization is required
 * if instances are shared across threads.
 */
public class ManagedVSR implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ManagedVSR.class);

    private final String id;
    private VectorSchemaRoot vsr;
    private final BufferAllocator allocator;
    private final AtomicReference<VSRState> state = new AtomicReference<>(VSRState.ACTIVE);
    private final Map<String, FieldVector> fields = new HashMap<>();

    /**
     * Creates a new ManagedVSR.
     *
     * @param id unique identifier for this VSR
     * @param schema Arrow schema defining the vector structure
     * @param allocator buffer allocator for Arrow memory
     */
    public ManagedVSR(String id, Schema schema, BufferAllocator allocator) {
        this.id = id;
        this.vsr = VectorSchemaRoot.create(schema, allocator);
        this.allocator = allocator;
        for (Field field : vsr.getSchema().getFields()) {
            fields.put(field.getName(), vsr.getVector(field));
        }
    }

    /** Returns the current row count. */
    public int getRowCount() {
        return vsr.getRowCount();
    }

    /**
     * Sets the row count.
     *
     * @param rowCount the new row count
     */
    public void setRowCount(int rowCount) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state.get());
        }
        vsr.setRowCount(rowCount);
    }

    /**
     * Returns the vector for the given field name, or null if not found.
     * @param fieldName the field name
     * @return the field vector, or null
     */
    public FieldVector getVector(String fieldName) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot access vector in VSR state: " + state.get());
        }
        return fields.get(fieldName);
    }

    /** Transitions this VSR from ACTIVE to FROZEN state. */
    public void moveToFrozen() {
        if (state.compareAndSet(VSRState.ACTIVE, VSRState.FROZEN) == false) {
            throw new IllegalStateException("Cannot freeze VSR " + id + ": expected ACTIVE but was " + state.get());
        }
        logger.debug("State transition: ACTIVE -> FROZEN for VSR {}", id);
    }

    /**
     * Exports this VSR to Arrow C Data Interface for native handoff.
     * Only allowed when VSR is FROZEN.
     */
    public ArrowExport exportToArrow() {
        if (state.get() != VSRState.FROZEN) {
            throw new IllegalStateException("Cannot export VSR in state: " + state.get() + ". Must be FROZEN.");
        }
        validateDeclaredSchemaMatchesVectors();
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);
        return new ArrowExport(arrowArray, arrowSchema);
    }

    /**
     * Verifies every declared field tree structurally matches its live vector tree before export.
     * <p>
     * {@code Data.exportVectorSchemaRoot} walks the DECLARED schema against field nodes unloaded
     * from the LIVE vectors. If the trees diverge (e.g. a schema refresh cached a child that was
     * never patched into the live struct), the walk misaligns: with differing child counts it
     * throws mid-load, and in Arrow 18.1.0 the partially-loaded struct copy is never closed —
     * only the record batch is (the try-with-resources covers {@code exportVector}, not
     * {@code StructVectorLoader.load}) — leaking its buffers on this VSR's allocator on every
     * attempt. Worse, with EQUAL child counts but different names the export would succeed and
     * silently write each child's data under the wrong column name. Failing fast here turns both
     * into a clear, allocation-free error.
     * <p>
     * Child names are only compared under a declared STRUCT parent: struct children are the trees
     * reconcile patches by name and downstream matches by position. List/map element names are
     * exempt — Arrow renames them internally ({@code $data$}/{@code entries}) without changing
     * what is exported (see {@link #refreshSchema}).
     */
    private void validateDeclaredSchemaMatchesVectors() {
        List<Field> declaredFields = vsr.getSchema().getFields();
        List<FieldVector> vectors = vsr.getFieldVectors();
        for (int i = 0; i < declaredFields.size(); i++) {
            requireMatchingShape(declaredFields.get(i), vectors.get(i).getField(), declaredFields.get(i).getName());
        }
    }

    private static void requireMatchingShape(Field declared, Field live, String path) {
        List<Field> declaredChildren = declared.getChildren();
        List<Field> liveChildren = live.getChildren();
        if (declaredChildren.size() != liveChildren.size()) {
            throw new IllegalStateException(
                "Cannot export VSR: declared schema for ["
                    + path
                    + "] has "
                    + declaredChildren.size()
                    + " children "
                    + fieldNames(declaredChildren)
                    + " but the live vector has "
                    + liveChildren.size()
                    + " "
                    + fieldNames(liveChildren)
                    + " — exporting would misalign field nodes and leak the partial export. "
                    + "The cached schema diverged from the live vectors (e.g. a partially-applied reconcile)."
            );
        }
        boolean namesMustMatch = declared.getType() instanceof ArrowType.Struct;
        for (int i = 0; i < declaredChildren.size(); i++) {
            Field declaredChild = declaredChildren.get(i);
            Field liveChild = liveChildren.get(i);
            if (namesMustMatch && declaredChild.getName().equals(liveChild.getName()) == false) {
                throw new IllegalStateException(
                    "Cannot export VSR: declared struct child ["
                        + path
                        + "."
                        + declaredChild.getName()
                        + "] does not match live child ["
                        + liveChild.getName()
                        + "] at position "
                        + i
                        + " — exporting would silently write data under the wrong column."
                );
            }
            requireMatchingShape(declaredChild, liveChild, path + "." + declaredChild.getName());
        }
    }

    private static String fieldNames(List<Field> fields) {
        return fields.stream().map(Field::getName).collect(java.util.stream.Collectors.toList()).toString();
    }

    /**
     * Exports only the schema to Arrow C Data Interface.
     */
    public ArrowSchema exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, vsr.getSchema(), null, arrowSchema);
        return arrowSchema;
    }

    /**
     * Returns the current lifecycle state.
     *
     * @return the VSR state
     */
    public VSRState getState() {
        return state.get();
    }

    /**
     * Dynamically adds a new field to this VSR. Creates the vector using the internal
     * allocator and appends it to the schema. Only allowed in ACTIVE state.
     *
     * @param field the Arrow field descriptor
     */
    public void addFieldVector(Field field) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot add field to VSR in state: " + state.get());
        }
        FieldVector vector = field.createVector(allocator);
        List<FieldVector> vectors = new ArrayList<>(vsr.getFieldVectors());
        vectors.add(vector);
        List<Field> newFields = new ArrayList<>(vsr.getSchema().getFields());
        newFields.add(field);
        int rowCount = vsr.getRowCount();
        vsr = new VectorSchemaRoot(newFields, vectors, rowCount);
        fields.put(field.getName(), vector);
    }

    /**
     * Rebuilds this VSR's {@link VectorSchemaRoot} so {@link #getSchema()} reflects a nested-child
     * mutation already applied directly to a live struct/map/list vector (e.g.
     * {@link VSRManager#reconcileSchema} patching a leaf into an active nested field). {@code
     * VectorSchemaRoot#getSchema()} is a snapshot fixed at the last construction/rebuild — it doesn't
     * track a struct's children afterward.
     * <p>
     * Re-derives the top-level field list via each top-level vector's own {@code getField()} rather
     * than the (stale) cached schema: {@code ListVector}/{@code StructVector#getField()} self-heals,
     * rebuilding from live children recursively, so calling it on every top-level vector picks up a
     * nested addition at any depth. Only allowed in ACTIVE state, mirroring {@link #addFieldVector}.
     */
    public void refreshSchema(Schema authoritative) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot refresh schema in VSR state: " + state.get());
        }
        List<FieldVector> vectors = vsr.getFieldVectors();
        List<Field> refreshedFields = new ArrayList<>(vectors.size());
        for (FieldVector vector : vectors) {
            // Prefer the authoritative (mapping-derived) declaration for the field: a vector's own
            // getField() reflects live children but also Arrow's internal child renames (a list's
            // child becomes "$data$"), which must not leak into the declared schema — the native
            // writer derives the Parquet leaf path (e.g. "tags.list.element") from declared names.
            Field declared = findByName(authoritative, vector.getName());
            refreshedFields.add(declared != null ? declared : vector.getField());
        }
        int rowCount = vsr.getRowCount();
        vsr = new VectorSchemaRoot(refreshedFields, vectors, rowCount);
    }

    private static Field findByName(Schema schema, String name) {
        for (Field field : schema.getFields()) {
            if (field.getName().equals(name)) {
                return field;
            }
        }
        return null;
    }

    /**
     * Returns the current Arrow schema of this VSR.
     *
     * @return the schema
     */
    public Schema getSchema() {
        return vsr.getSchema();
    }

    /**
     * Returns the unique identifier.
     *
     * @return the VSR id
     */
    public String getId() {
        return id;
    }

    @Override
    public void close() {
        if (state.get() == VSRState.CLOSED) {
            return;
        }
        if (state.get() == VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot close VSR " + id + ": must freeze first");
        }
        if (state.compareAndSet(VSRState.FROZEN, VSRState.CLOSED) == false) {
            throw new IllegalStateException("Expected VSR to be FROZEN but was " + state.get());
        }
        logger.debug("State transition: FROZEN -> CLOSED for VSR {}", id);
        if (vsr != null) {
            vsr.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Override
    public String toString() {
        return "ManagedVSR{id='" + id + "', state=" + state.get() + ", rows=" + getRowCount() + "}";
    }
}
