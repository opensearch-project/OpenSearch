/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.rest.RestStatus;

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

    private static final Logger logger = LogManager.getLogger(NativeErrorConverter.class);

    private NativeErrorConverter() {}

    /**
     * Logs the verbose native allocator dump ("top memory consumers ... query_untracked(partitions=N,batch=8192)#id
     * consumed X MB", reservation ids, plan-operator internals) server-side at WARN. It is useful for operators
     * but must not reach the user, so the converted exception carries no cause — its own clean message is the
     * whole user-facing story. {@code label} identifies the conversion for the server log.
     */
    private static void logRawNativeError(MatchedError match, String label) {
        logger.warn("[NativeErrorConverter] {} — raw native error: {}", label, match.message());
    }

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

    private static final String ADMISSION_REJECTED_MSG = "Native query admission rejected: insufficient memory budget available";

    /**
     * Registered error patterns, checked in order. First match wins.
     * Key phrases include both the Rust-originated messages (for data-node local conversion)
     * and the controlled output messages (for coordinator-side conversion after transport).
     *
     * <p>Order matters: more-specific prefixes must precede less-specific ones.
     * {@code "[analytics_backend_datafusion] Failed to allocate"} must come before bare
     * {@code "Failed to allocate"}.
     */
    /**
     * Maximum depth to walk the exception cause chain when matching error patterns.
     */
    static final int MAX_CAUSE_CHAIN_DEPTH = 10;

    private static final List<ErrorPattern> PATTERNS = List.of(
        new ErrorPattern("Cannot reserve untracked memory budget", NativeErrorConverter::convertAdmissionRejection),
        new ErrorPattern(ADMISSION_REJECTED_MSG, NativeErrorConverter::convertAdmissionRejection),
        new ErrorPattern("[analytics_backend_datafusion] Failed to allocate", NativeErrorConverter::convertPoolLimitFromControlled),
        new ErrorPattern("Failed to allocate", NativeErrorConverter::convertPoolLimitExceeded),
        // DataFusion's own spill-path error when an operator can't allocate and DiskManager is disabled.
        // Semantically a memory-pressure error → CircuitBreakingException (HTTP 429).
        new ErrorPattern("Memory Exhausted while", NativeErrorConverter::convertSpillPoolExhausted),
        // Raw prost decoder error when a Substrait plan exceeds the protobuf recursion limit
        // (deeply nested function calls). Converted at the FFM boundary into a clean 400.
        new ErrorPattern("recursion limit reached", NativeErrorConverter::convertRecursionLimit),
        // Controlled message, as a coordinator-side safety net if it arrives via StreamException.
        new ErrorPattern("Query too deeply nested", NativeErrorConverter::convertRecursionLimit)
    );

    /**
     * Attempts to convert a native error into an appropriate OpenSearch exception.
     * Walks the cause chain looking for messages matching registered patterns.
     * Returns the original exception unchanged if no pattern matches.
     */
    public static Exception convert(Exception original) {
        // Don't re-convert if already a recognized exception type
        if (original instanceof CircuitBreakingException || original instanceof OpenSearchStatusException) {
            return original;
        }
        Throwable current = original;
        int depth = 0;
        while (current != null && depth < MAX_CAUSE_CHAIN_DEPTH) {
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
        logRawNativeError(match, "pool limit exceeded");
        String message = "[analytics_backend_datafusion] Failed to allocate " + parsed[0] + " bytes (limit: " + parsed[1] + ")";
        // No cause attached: the raw native error carries the allocator dump and stays in the server log only.
        return new CircuitBreakingException(message, parsed[0], parsed[1], CircuitBreaker.Durability.TRANSIENT);
    }

    private static Exception convertPoolLimitFromControlled(MatchedError match) {
        long[] parsed = parseControlledPoolLimitBytes(match.message());
        if (parsed == null) {
            return match.original();
        }
        logRawNativeError(match, "pool limit exceeded (controlled)");
        // Rebuild the message from the parsed numbers only (the matched message may carry an appended
        // native consumer dump); never echo match.message() verbatim.
        String message = "[analytics_backend_datafusion] Failed to allocate " + parsed[0] + " bytes (limit: " + parsed[1] + ")";
        return new CircuitBreakingException(message, parsed[0], parsed[1], CircuitBreaker.Durability.TRANSIENT);
    }

    private static Exception convertAdmissionRejection(MatchedError match) {
        // Log the raw native budget detail server-side; don't attach it as a user-facing cause.
        logRawNativeError(match, "admission rejected");
        return new OpenSearchStatusException(ADMISSION_REJECTED_MSG, RestStatus.TOO_MANY_REQUESTS);
    }

    private static Exception convertSpillPoolExhausted(MatchedError match) {
        logRawNativeError(match, "spill pool exhausted");
        // Bytes/limit aren't part of this DataFusion message; surface 0/0 to keep the type contract.
        // Use a fixed clean message — the raw DataFusion text can carry operator internals.
        return new CircuitBreakingException(
            "[analytics_backend_datafusion] memory pool exhausted (spill unavailable)",
            0L,
            0L,
            CircuitBreaker.Durability.TRANSIENT
        );
    }

    private static Exception convertRecursionLimit(MatchedError match) {
        return new IllegalArgumentException(
            "Query too deeply nested: the expression exceeds the maximum nesting depth supported by the execution engine. "
                + "Simplify the query by reducing nested function calls."
        );
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
     * Matches the controlled pool limit message produced by this converter:
     * "[analytics_backend_datafusion] Failed to allocate {N} bytes (limit: {L})"
     */
    private static final Pattern CONTROLLED_POOL_LIMIT_PATTERN = Pattern.compile(
        "\\[analytics_backend_datafusion] Failed to allocate (\\d+) bytes \\(limit: (\\d+)\\)"
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
        return parseLongPair(m);
    }

    private static long[] parseControlledPoolLimitBytes(String msg) {
        Matcher m = CONTROLLED_POOL_LIMIT_PATTERN.matcher(msg);
        if (m.find() == false) {
            return null;
        }
        return parseLongPair(m);
    }

    private static long[] parseLongPair(Matcher m) {
        try {
            long bytesRequested = Long.parseLong(m.group(1));
            long limit = Long.parseLong(m.group(2));
            return new long[] { bytesRequested, limit };
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
