/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts native (Rust) errors into appropriate OpenSearch exception types.
 *
 * <p>Rust-side errors cross the FFM boundary as {@code RuntimeException(message)}.
 * This converter walks the exception cause chain, matches against known error
 * patterns (defined in Rust's {@code native_error.rs}), and produces the
 * corresponding OpenSearch exception with correct HTTP status semantics.
 *
 * <h2>Adding new error conversions</h2>
 * <ol>
 *   <li>Add a factory function in Rust {@code native_error.rs} with a stable key phrase</li>
 *   <li>Add a {@link ErrorPattern} entry to {@link #PATTERNS} below</li>
 * </ol>
 *
 * @see <a href="sandbox/plugins/analytics-backend-datafusion/rust/src/native_error.rs">native_error.rs</a>
 */
public final class NativeErrorConverter {

    private NativeErrorConverter() {}

    /**
     * A pattern that matches a native error message and converts it to an OpenSearch exception.
     */
    private record ErrorPattern(String keyPhrase, Function<MatchedError, Exception> converter) {
    }

    /**
     * Carries the matched message and original exception for converter functions.
     */
    private record MatchedError(String message, Exception original) {
    }

    /**
     * Registered error patterns, checked in order. First match wins.
     * Key phrases correspond to stable prefixes in Rust native_error.rs.
     */
    private static final List<ErrorPattern> PATTERNS = List.of(
        new ErrorPattern("Cannot reserve untracked memory budget", NativeErrorConverter::convertAdmissionRejection),
        new ErrorPattern("Failed to allocate", NativeErrorConverter::convertPoolLimitExceeded)
    );

    /**
     * Attempts to convert a native error into an appropriate OpenSearch exception.
     * Walks the cause chain looking for messages matching registered patterns.
     * Returns the original exception unchanged if no pattern matches.
     */
    public static Exception convert(Exception original) {
        // Don't re-convert if already a recognized exception type
        if (original instanceof CircuitBreakingException || original instanceof OpenSearchRejectedExecutionException) {
            return original;
        }
        Throwable current = original;
        int depth = 0;
        while (current != null && depth < 10) {
            String msg = current.getMessage();
            if (msg != null) {
                for (ErrorPattern pattern : PATTERNS) {
                    if (msg.contains(pattern.keyPhrase())) {
                        return pattern.converter().apply(new MatchedError(msg, original));
                    }
                }
            }
            current = current.getCause();
            depth++;
        }
        return original;
    }

    // ─── Converter functions ────────────────────────────────────────────────────

    private static Exception convertPoolLimitExceeded(MatchedError match) {
        long[] parsed = parsePoolLimitBytes(match.message());
        if (parsed == null) {
            return match.original();
        }
        String message = match.message().contains("[native_request]") ? match.message() : "[native_request] " + match.message();
        CircuitBreakingException cbe = new CircuitBreakingException(message, parsed[0], parsed[1], CircuitBreaker.Durability.TRANSIENT);
        cbe.initCause(match.original());
        return cbe;
    }

    private static Exception convertAdmissionRejection(MatchedError match) {
        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException(
            "Native query admission rejected: " + match.message(),
            true
        );
        rejection.initCause(match.original());
        return rejection;
    }

    // ─── Message parsing ────────────────────────────────────────────────────────

    /**
     * Matches the pool limit error format from native_error.rs:
     * "Failed to allocate {N} bytes for {consumer} ({reserved} already reserved) — {available} available out of {limit} limit"
     */
    private static final Pattern POOL_LIMIT_PATTERN = Pattern.compile(
        "Failed to allocate (\\d+) bytes for .+ \\(\\d+ already reserved\\) — \\d+ available out of (\\d+) limit"
    );

    /**
     * Parses bytes_requested and limit from pool limit error message using regex.
     * Returns [bytesRequested, limit] or null if the message doesn't match the expected format.
     */
    private static long[] parsePoolLimitBytes(String msg) {
        Matcher m = POOL_LIMIT_PATTERN.matcher(msg);
        if (m.find() == false) {
            return null;
        }
        try {
            long bytesRequested = Long.parseLong(m.group(1));
            long limit = Long.parseLong(m.group(2));
            return new long[] { bytesRequested, limit };
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
