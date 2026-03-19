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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.parquet.bridge.ArrowExport;

import java.util.HashMap;
import java.util.Map;

/**
 * Managed wrapper around VectorSchemaRoot that handles ACTIVE/FROZEN/CLOSED lifecycle.
 */
public class ManagedVSR implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ManagedVSR.class);

    private final String id;
    private final VectorSchemaRoot vsr;
    private final BufferAllocator allocator;
    private VSRState state;
    private final Map<String, Field> fields = new HashMap<>();

    public ManagedVSR(String id, Schema schema, BufferAllocator allocator) {
        this.id = id;
        this.vsr = VectorSchemaRoot.create(schema, allocator);
        this.allocator = allocator;
        this.state = VSRState.ACTIVE;
        for (Field field : vsr.getSchema().getFields()) {
            fields.put(field.getName(), field);
        }
    }

    public int getRowCount() {
        return vsr.getRowCount();
    }

    public void setRowCount(int rowCount) {
        if (state != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state);
        }
        vsr.setRowCount(rowCount);
    }

    public FieldVector getVector(String fieldName) {
        if (state != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot access vector in VSR state: " + state);
        }
        Field field = fields.get(fieldName);
        return field != null ? vsr.getVector(field) : null;
    }

    public void moveToFrozen() {
        if (state != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot freeze VSR " + id + ": expected ACTIVE but was " + state);
        }
        state = VSRState.FROZEN;
        logger.debug("State transition: ACTIVE -> FROZEN for VSR {}", id);
    }

    /**
     * Exports this VSR to Arrow C Data Interface for native handoff.
     * Only allowed when VSR is FROZEN.
     */
    public ArrowExport exportToArrow() {
        if (state != VSRState.FROZEN) {
            throw new IllegalStateException("Cannot export VSR in state: " + state + ". Must be FROZEN.");
        }
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);
        return new ArrowExport(arrowArray, arrowSchema);
    }

    /**
     * Exports only the schema to Arrow C Data Interface.
     */
    public ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, vsr.getSchema(), null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    public VSRState getState() {
        return state;
    }

    public String getId() {
        return id;
    }

    @Override
    public void close() {
        if (state == VSRState.CLOSED) {
            return;
        }
        if (state == VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot close VSR " + id + ": must freeze first");
        }
        state = VSRState.CLOSED;
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
        return String.format("ManagedVSR{id='%s', state=%s, rows=%d}", id, state, getRowCount());
    }
}
