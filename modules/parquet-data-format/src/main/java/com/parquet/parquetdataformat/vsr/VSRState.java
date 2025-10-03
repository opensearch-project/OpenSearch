package com.parquet.parquetdataformat.vsr;

/**
 * Represents the lifecycle states of a VectorSchemaRoot in the Project Mustang
 * Parquet Writer Plugin architecture.
 */
public enum VSRState {
    /**
     * Currently accepting writes - the VSR is active and can be modified.
     */
    ACTIVE,
    
    /**
     * Read-only state - VSR is frozen and queued for flush to Rust.
     * No further modifications are allowed in this state.
     */
    FROZEN,
    
    /**
     * Currently being processed by Rust - VSR is in the handoff process.
     */
    FLUSHING,
    
    /**
     * Completed and cleaned up - VSR processing is complete and resources freed.
     */
    CLOSED
}
