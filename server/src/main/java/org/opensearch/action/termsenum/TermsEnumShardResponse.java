/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * What one shard found: one of four typed outcomes.
 *
 * @opensearch.internal
 */
public final class TermsEnumShardResponse extends ActionResponse {

    /**
     * The result of enumerating one shard.
     *
     * @opensearch.internal
     */
    public enum Outcome {
        /** The shard listed terms, possibly cut short by its deadline. */
        OK,
        /** The index filter ruled the shard out. */
        SKIPPED,
        /** The field is unmapped or hidden, so the shard has no terms. */
        FIELD_ABSENT,
        /** The field exists but its type cannot be enumerated. */
        FIELD_UNSUPPORTED
    }

    private final Outcome outcome;
    private final List<String> terms;
    private final boolean timedOut;
    private final String reason;

    private TermsEnumShardResponse(Outcome outcome, List<String> terms, boolean timedOut, @Nullable String reason) {
        this.outcome = outcome;
        this.terms = terms;
        this.timedOut = timedOut;
        this.reason = reason;
    }

    /**
     * Reads a response from the wire.
     *
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public TermsEnumShardResponse(StreamInput in) throws IOException {
        super(in);
        outcome = in.readEnum(Outcome.class);
        terms = in.readStringList();
        timedOut = in.readBoolean();
        reason = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(outcome);
        out.writeStringCollection(terms);
        out.writeBoolean(timedOut);
        out.writeOptionalString(reason);
    }

    /**
     * Creates an {@link Outcome#OK} response.
     *
     * @param terms the sorted, distinct terms
     * @param timedOut whether the shard stopped at its deadline
     * @return the response
     */
    public static TermsEnumShardResponse ok(List<String> terms, boolean timedOut) {
        return new TermsEnumShardResponse(Outcome.OK, List.copyOf(terms), timedOut, null);
    }

    /** @return a {@link Outcome#SKIPPED} response */
    public static TermsEnumShardResponse skipped() {
        return new TermsEnumShardResponse(Outcome.SKIPPED, List.of(), false, null);
    }

    /** @return a {@link Outcome#FIELD_ABSENT} response */
    public static TermsEnumShardResponse fieldAbsent() {
        return new TermsEnumShardResponse(Outcome.FIELD_ABSENT, List.of(), false, null);
    }

    /**
     * Creates a {@link Outcome#FIELD_UNSUPPORTED} response.
     *
     * @param reason why the field cannot be enumerated
     * @return the response
     */
    public static TermsEnumShardResponse fieldUnsupported(String reason) {
        return new TermsEnumShardResponse(Outcome.FIELD_UNSUPPORTED, List.of(), false, Objects.requireNonNull(reason));
    }

    /** @return the outcome */
    public Outcome outcome() {
        return outcome;
    }

    /** @return the terms, empty unless the outcome is {@link Outcome#OK} */
    public List<String> terms() {
        return terms;
    }

    /** @return whether the shard stopped at its deadline */
    public boolean timedOut() {
        return timedOut;
    }

    /** @return why the field is unsupported, or {@code null} for other outcomes */
    @Nullable
    public String reason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TermsEnumShardResponse that = (TermsEnumShardResponse) o;
        return timedOut == that.timedOut && outcome == that.outcome && terms.equals(that.terms) && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, terms, timedOut, reason);
    }
}
