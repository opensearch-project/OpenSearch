/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Slot labels for the hash-shuffle transport. A <em>slot</em> identifies one of a shuffle
 * consumer's independent input streams: producers stamp their outgoing payloads with it and the
 * consumer's buffer keeps one accumulation + completion latch per slot.
 *
 * <p>The transport is N-ary — a consumer may have any number of slots — but a two-input consumer
 * (the binary hash join, by far the common case) uses the historical {@link #LEFT} / {@link #RIGHT}
 * labels so its wire payloads, buffer keys and spill file names are unchanged. Only a consumer with
 * three or more inputs uses the positional {@code "in<index>"} form.
 *
 * @opensearch.internal
 */
public final class ShuffleSlots {

    /** Slot label for a binary consumer's first (probe) input. Also the single slot of a
     *  one-input consumer such as the FINAL-aggregate shuffle worker. */
    public static final String LEFT = "left";

    /** Slot label for a binary consumer's second (build) input. */
    public static final String RIGHT = "right";

    private ShuffleSlots() {}

    /**
     * The slot label for input {@code index} of an {@code arity}-input consumer. Arity 1 and 2 map
     * to {@link #LEFT} / {@link #RIGHT} so the binary path keeps its historical labels; higher
     * arities use {@code "in<index>"}.
     *
     * @throws IllegalArgumentException if {@code index} is outside {@code [0, arity)}
     */
    public static String forInput(int index, int arity) {
        if (index < 0 || index >= arity) {
            throw new IllegalArgumentException("Shuffle slot index " + index + " is outside [0," + arity + ")");
        }
        if (arity <= 2) {
            return index == 0 ? LEFT : RIGHT;
        }
        return "in" + index;
    }

    /**
     * Validates a slot label. Labels are engine-generated, but they key the consumer buffer's
     * per-slot state AND name its on-disk spill file, so a label containing a path separator or
     * {@code ..} would let a future caller escape the spill directory. Reject anything that is not
     * a simple alphanumeric/dash/underscore token.
     *
     * @return {@code slot}, for use in a fluent assignment
     * @throws IllegalArgumentException if {@code slot} is null, empty, or not a safe token
     */
    public static String validate(String slot) {
        if (slot == null || slot.isEmpty()) {
            throw new IllegalArgumentException("Shuffle slot label must be non-empty");
        }
        for (int i = 0; i < slot.length(); i++) {
            char c = slot.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
            if (!ok) {
                throw new IllegalArgumentException("Shuffle slot label '" + slot + "' must contain only [A-Za-z0-9_-]");
            }
        }
        return slot;
    }
}
