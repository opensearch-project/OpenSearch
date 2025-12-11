package com.parquet.parquetdataformat.vsr;

import java.util.EnumSet;
import java.util.Set;

/**
 * Represents the lifecycle states of a VectorSchemaRoot.
 * Valid transitions: ACTIVE -> FROZEN -> FLUSHING -> CLOSED
 */
public enum VSRState {
    ACTIVE,
    FROZEN,
    FLUSHING,
    CLOSED;

    private static final Set<VSRState> ACTIVE_VALID_NEXT = EnumSet.of(FROZEN, CLOSED);
    private static final Set<VSRState> FROZEN_VALID_NEXT = EnumSet.of(FLUSHING, CLOSED);
    private static final Set<VSRState> FLUSHING_VALID_NEXT = EnumSet.of(CLOSED);
    private static final Set<VSRState> CLOSED_VALID_NEXT = EnumSet.noneOf(VSRState.class);

    public boolean canTransitionTo(VSRState next) {
        switch (this) {
            case ACTIVE: return ACTIVE_VALID_NEXT.contains(next);
            case FROZEN: return FROZEN_VALID_NEXT.contains(next);
            case FLUSHING: return FLUSHING_VALID_NEXT.contains(next);
            case CLOSED: return CLOSED_VALID_NEXT.contains(next);
            default: return false;
        }
    }

    public void validateTransition(VSRState next) {
        if (!canTransitionTo(next)) {
            throw new IllegalStateException(
                String.format("Invalid state transition: %s -> %s", this, next)
            );
        }
    }
}
