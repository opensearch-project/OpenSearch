/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.StageExecution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Wires the child→parent state cascade and the reverse parent→sibling cancel sweep.
 *
 * <p>Per-child: SUCCEEDED decrements a shared counter and schedules parent on zero;
 * FAILED propagates via {@code failWithCause}; CANCELLED is ignored (the initiator
 * already owns the parent's lifecycle).
 *
 * <p>Parent: on FAILED / CANCELLED, sweep still-running children — they shouldn't
 * keep producing into a sink whose owner has terminated.
 *
 * <p>Thread-safe under the documented stage contracts: pending counter is atomic;
 * child state reads + cancel are idempotent; {@code wire} runs during graph build
 * (before any transitions fire), so listener registration never races with firing.
 *
 * @opensearch.internal
 */
final class StageListenerWiring {

    private StageListenerWiring() {}

    /** Children must have their own state listeners attached first — fires in registration order. */
    static void wire(Consumer<StageExecution> scheduler, StageExecution parent, List<? extends StageExecution> children) {
        if (children.isEmpty()) return;

        AtomicInteger pending = new AtomicInteger(children.size());
        for (StageExecution child : children) {
            child.addStateListener((from, to) -> {
                switch (to) {
                    case SUCCEEDED -> {
                        if (pending.decrementAndGet() == 0) {
                            handOffMetadataAndSchedule(scheduler, parent, children);
                        }
                    }
                    case FAILED -> {
                        Exception cause = child.getFailure();
                        parent.failWithCause(
                            cause != null
                                ? cause
                                : new RuntimeException("child stage " + child.getStageId() + " failed without recorded cause")
                        );
                    }
                    default -> {
                    }  // CANCELLED intentionally not propagated
                }
            });
        }

        // child.cancel is a no-op when already terminal, so this is safe under top-down sweeps.
        parent.addStateListener((from, to) -> {
            if (to == StageExecution.State.FAILED || to == StageExecution.State.CANCELLED) {
                for (StageExecution child : children) {
                    if (child.getState().isTerminal() == false) {
                        child.cancel("parent " + parent.getStageId() + " " + to);
                    }
                }
            }
        });
    }

    /** Collects {@code publishedMetadata} from each child, hands to parent, then schedules. */
    private static void handOffMetadataAndSchedule(
        Consumer<StageExecution> scheduler,
        StageExecution parent,
        List<? extends StageExecution> children
    ) {
        Map<Integer, Object> metadata = new HashMap<>();
        for (StageExecution child : children) {
            Object payload = child.publishedMetadata();
            if (payload != null) {
                metadata.put(child.getStageId(), payload);
            }
        }
        parent.consumeChildMetadata(metadata);
        scheduler.accept(parent);
    }
}
