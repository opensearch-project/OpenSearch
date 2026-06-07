/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import java.util.Locale;

/**
 * Maps a child stage's producer count (shard count for SHARD_FRAGMENT children) to
 * the number of native partition-stream lanes the coordinator-reduce sink registers.
 * Default {@code per_shard} gives one lane per shard so DataFusion's
 * {@code StreamingTable} consumes each shard on its own partition worker without an
 * intermediate {@code RepartitionExec}.
 *
 * <ul>
 *   <li>{@code per_shard} — lanes = numProducers (default).</li>
 *   <li>{@code single} — lanes = 1. Kill switch for the per-shard path.</li>
 *   <li>{@code cap:N} — lanes = min(numProducers, N).</li>
 *   <li>{@code ratio:N} — lanes = max(1, numProducers / N). N producers share a lane.</li>
 * </ul>
 *
 * <p>{@link #parse(String)} is whitespace- and case-insensitive; invalid strings throw.
 */
public final class PartitionLanePolicy {

    /** One lane per producer (default — per-shard partition stream). */
    public static final PartitionLanePolicy PER_SHARD = new PartitionLanePolicy(Kind.PER_SHARD, 0);
    /** Always one lane (legacy single-mpsc fallback). */
    public static final PartitionLanePolicy SINGLE = new PartitionLanePolicy(Kind.SINGLE, 0);

    /** Default policy string used by the cluster setting. */
    public static final String DEFAULT_VALUE = "per_shard";

    /** Hard upper bound on the {@code N} parameter for {@code cap:N} / {@code ratio:N}. */
    static final int MAX_PARAMETER = 1024;

    /** How coordinator reduce lanes map to shards: one per shard, a single merged lane, a fixed cap, or a ratio of shards. */
    public enum Kind {
        PER_SHARD,
        SINGLE,
        CAP,
        RATIO
    }

    private final Kind kind;
    private final int parameter;

    private PartitionLanePolicy(Kind kind, int parameter) {
        this.kind = kind;
        this.parameter = parameter;
    }

    /** Returns a {@code cap:n} policy: lanes capped at {@code n}. */
    public static PartitionLanePolicy cap(int n) {
        validateParameter(n, "cap");
        return new PartitionLanePolicy(Kind.CAP, n);
    }

    /** Returns a {@code ratio:n} policy: {@code n} producers share one lane. */
    public static PartitionLanePolicy ratio(int n) {
        validateParameter(n, "ratio");
        return new PartitionLanePolicy(Kind.RATIO, n);
    }

    private static void validateParameter(int n, String label) {
        if (n < 1) {
            throw new IllegalArgumentException("partition lane policy '" + label + "' parameter must be >= 1, got " + n);
        }
        if (n > MAX_PARAMETER) {
            throw new IllegalArgumentException(
                "partition lane policy '" + label + "' parameter must be <= " + MAX_PARAMETER + ", got " + n
            );
        }
    }

    public Kind kind() {
        return kind;
    }

    public int parameter() {
        return parameter;
    }

    /**
     * Returns the lane count for a child input. Always {@code >= 1} — DataFusion's
     * {@code StreamingTable} rejects zero-partition registrations.
     */
    public int resolveLanes(int numProducers) {
        if (numProducers < 1) {
            return 1;
        }
        return switch (kind) {
            case PER_SHARD -> numProducers;
            case SINGLE -> 1;
            case CAP -> Math.min(numProducers, parameter);
            case RATIO -> Math.max(1, numProducers / parameter);
        };
    }

    /**
     * Parses a policy string. Accepts {@code "per_shard"}, {@code "single"},
     * {@code "cap:N"}, {@code "ratio:N"} (case- and whitespace-insensitive on
     * both sides of the colon).
     *
     * @throws IllegalArgumentException for unrecognised strings or out-of-range parameters
     */
    public static PartitionLanePolicy parse(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("partition lane policy string must not be null");
        }
        String trimmed = raw.trim().toLowerCase(Locale.ROOT);
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("partition lane policy string must not be empty");
        }
        if (trimmed.equals("per_shard")) {
            return PER_SHARD;
        }
        if (trimmed.equals("single")) {
            return SINGLE;
        }
        int colon = trimmed.indexOf(':');
        if (colon > 0 && colon < trimmed.length() - 1) {
            String head = trimmed.substring(0, colon).trim();
            String tail = trimmed.substring(colon + 1).trim();
            int value;
            try {
                value = Integer.parseInt(tail);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "partition lane policy '" + head + "' requires an integer parameter, got '" + tail + "'"
                );
            }
            return switch (head) {
                case "cap" -> cap(value);
                case "ratio" -> ratio(value);
                default -> throw new IllegalArgumentException(
                    "unknown partition lane policy '" + head + "'; expected one of [per_shard, single, cap:N, ratio:N], got '" + raw + "'"
                );
            };
        }
        throw new IllegalArgumentException(
            "unknown partition lane policy; expected one of [per_shard, single, cap:N, ratio:N], got '" + raw + "'"
        );
    }

    /** Wire-format string round-trippable through {@link #parse(String)}. */
    public String toWireString() {
        return switch (kind) {
            case PER_SHARD -> "per_shard";
            case SINGLE -> "single";
            case CAP -> "cap:" + parameter;
            case RATIO -> "ratio:" + parameter;
        };
    }

    @Override
    public String toString() {
        return "PartitionLanePolicy{" + toWireString() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionLanePolicy other)) return false;
        return kind == other.kind && parameter == other.parameter;
    }

    @Override
    public int hashCode() {
        return kind.hashCode() * 31 + parameter;
    }
}
