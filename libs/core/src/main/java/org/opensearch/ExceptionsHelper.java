/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch;

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.compress.NotXContentException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.OpenSearchException.getExceptionSimpleClassName;

/**
 * Helper class for OpenSearch Exceptions
 *
 * @opensearch.internal
 */
public final class ExceptionsHelper {
    private static final Logger logger = LogManager.getLogger(ExceptionsHelper.class);

    // utility class: no ctor
    private ExceptionsHelper() {}

    public static RuntimeException convertToRuntime(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new OpenSearchException(e);
    }

    public static OpenSearchException convertToOpenSearchException(Exception e) {
        if (e instanceof OpenSearchException) {
            return (OpenSearchException) e;
        }
        return new OpenSearchException(e);
    }

    public static RestStatus status(Throwable t) {
        if (t != null) {
            if (t instanceof OpenSearchException) {
                return ((OpenSearchException) t).status();
            } else if (t instanceof IllegalArgumentException) {
                return RestStatus.BAD_REQUEST;
            } else if (t instanceof JsonParseException) {
                return RestStatus.BAD_REQUEST;
            } else if (t instanceof OpenSearchRejectedExecutionException) {
                return RestStatus.TOO_MANY_REQUESTS;
            } else if (t instanceof NotXContentException) {
                return RestStatus.BAD_REQUEST;
            }
        }
        return RestStatus.INTERNAL_SERVER_ERROR;
    }

    public static String summaryMessage(Throwable t) {
        if (t != null) {
            if (t instanceof OpenSearchException) {
                return getExceptionSimpleClassName(t) + "[" + t.getMessage() + "]";
            } else if (t instanceof IllegalArgumentException) {
                return "Invalid argument";
            } else if (t instanceof JsonParseException) {
                return "Failed to parse JSON";
            } else if (t instanceof OpenSearchRejectedExecutionException) {
                return "Too many requests";
            }
        }
        return "Internal failure";
    }

    public static Throwable unwrapCause(Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result instanceof OpenSearchWrapperException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            if (counter++ > 10) {
                // dear god, if we got more than 10 levels down, WTF? just bail
                logger.warn("Exception cause unwrapping ran for 10 levels...", t);
                return result;
            }
            result = result.getCause();
        }
        return result;
    }

    /**
     * @deprecated Don't swallow exceptions, allow them to propagate.
     */
    @Deprecated
    public static String detailedMessage(Throwable t) {
        if (t == null) {
            return "Unknown";
        }
        if (t.getCause() != null) {
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                sb.append(getExceptionSimpleClassName(t));
                if (t.getMessage() != null) {
                    sb.append("[");
                    sb.append(t.getMessage());
                    sb.append("]");
                }
                sb.append("; ");
                t = t.getCause();
                if (t != null) {
                    sb.append("nested: ");
                }
            }
            return sb.toString();
        } else {
            return getExceptionSimpleClassName(t) + "[" + t.getMessage() + "]";
        }
    }

    public static String stackTrace(Throwable e) {
        StringWriter stackTraceStringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stackTraceStringWriter);
        e.printStackTrace(printWriter);
        return stackTraceStringWriter.toString();
    }

    public static String formatStackTrace(final StackTraceElement[] stackTrace) {
        return Arrays.stream(stackTrace).skip(1).map(e -> "\tat " + e).collect(Collectors.joining("\n"));
    }

    /**
     * Rethrows the first exception in the list and adds all remaining to the suppressed list.
     * If the given list is empty no exception is thrown
     *
     */
    public static <T extends Throwable> void rethrowAndSuppress(List<T> exceptions) throws T {
        T main = null;
        for (T ex : exceptions) {
            main = useOrSuppress(main, ex);
        }
        if (main != null) {
            throw main;
        }
    }

    /**
     * Throws a runtime exception with all given exceptions added as suppressed.
     * If the given list is empty no exception is thrown
     */
    public static <T extends Throwable> void maybeThrowRuntimeAndSuppress(List<T> exceptions) {
        T main = null;
        for (T ex : exceptions) {
            main = useOrSuppress(main, ex);
        }
        if (main != null) {
            throw new OpenSearchException(main);
        }
    }

    public static <T extends Throwable> T useOrSuppress(T first, T second) {
        if (first == null) {
            return second;
        } else {
            first.addSuppressed(second);
        }
        return first;
    }

    private static final List<Class<? extends IOException>> CORRUPTION_EXCEPTIONS = Arrays.asList(
        CorruptIndexException.class,
        IndexFormatTooOldException.class,
        IndexFormatTooNewException.class
    );

    /**
     * Looks at the given Throwable's and its cause(s) as well as any suppressed exceptions on the Throwable as well as its causes
     * and returns the first corruption indicating exception (as defined by {@link #CORRUPTION_EXCEPTIONS}) it finds.
     * @param t Throwable
     * @return Corruption indicating exception if one is found, otherwise {@code null}
     */
    public static IOException unwrapCorruption(Throwable t) {
        return t == null ? null : ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(t, cause -> {
            for (Class<?> clazz : CORRUPTION_EXCEPTIONS) {
                if (clazz.isInstance(cause)) {
                    return true;
                }
            }
            return false;
        }).orElse(null);
    }

    /**
     * Looks at the given Throwable and its cause(s) and returns the first Throwable that is of one of the given classes or {@code null}
     * if no matching Throwable is found. Unlike {@link #unwrapCorruption} this method does only check the given Throwable and its causes
     * but does not look at any suppressed exceptions.
     * @param t Throwable
     * @param clazzes Classes to look for
     * @return Matching Throwable if one is found, otherwise {@code null}
     */
    public static Throwable unwrap(Throwable t, Class<?>... clazzes) {
        if (t != null) {
            final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            do {
                if (seen.add(t) == false) {
                    return null;
                }
                for (Class<?> clazz : clazzes) {
                    if (clazz.isInstance(t)) {
                        return t;
                    }
                }
            } while ((t = t.getCause()) != null);
        }
        return null;
    }

    /**
     * Throws the specified exception. If null if specified then <code>true</code> is returned.
     */
    public static boolean reThrowIfNotNull(@Nullable Throwable e) {
        if (e != null) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable> Optional<T> unwrapCausesAndSuppressed(Throwable cause, Predicate<Throwable> predicate) {
        if (predicate.test(cause)) {
            return Optional.of((T) cause);
        }

        final Queue<Throwable> queue = new LinkedList<>();
        queue.add(cause);
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (queue.isEmpty() == false) {
            final Throwable current = queue.remove();
            if (seen.add(current) == false) {
                continue;
            }
            if (predicate.test(current)) {
                return Optional.of((T) current);
            }
            Collections.addAll(queue, current.getSuppressed());
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
        }
        return Optional.empty();
    }

    /**
     * Unwrap the specified throwable looking for any suppressed errors or errors as a root cause of the specified throwable.
     *
     * @param cause the root throwable
     * @return an optional error if one is found suppressed or a root cause in the tree rooted at the specified throwable
     */
    public static Optional<Error> maybeError(final Throwable cause) {
        return unwrapCausesAndSuppressed(cause, t -> t instanceof Error);
    }

    /**
     * If the specified cause is an unrecoverable error, this method will rethrow the cause on a separate thread so that it can not be
     * caught and bubbles up to the uncaught exception handler. Note that the cause tree is examined for any {@link Error}. See
     * {@link #maybeError(Throwable)} for the semantics.
     *
     * @param throwable the throwable to possibly throw on another thread
     */
    public static void maybeDieOnAnotherThread(final Throwable throwable) {
        ExceptionsHelper.maybeError(throwable).ifPresent(error -> {
            /*
             * Here be dragons. We want to rethrow this so that it bubbles up to the uncaught exception handler. Yet, sometimes the stack
             * contains statements that catch any throwable (e.g., Netty, and the JDK futures framework). This means that a rethrow here
             * will not bubble up to where we want it to. So, we fork a thread and throw the exception from there where we are sure the
             * stack does not contain statements that catch any throwable. We do not wrap the exception so as to not lose the original cause
             * during exit.
             */
            try {
                // try to log the current stack trace
                final String formatted = ExceptionsHelper.formatStackTrace(Thread.currentThread().getStackTrace());
                logger.error("fatal error\n{}", formatted);
            } finally {
                new Thread(() -> { throw error; }).start();
            }
        });
    }

    /**
     * Run passed runnable and catch exception and translate exception into runtime exception using
     * {@link ExceptionsHelper#convertToRuntime(Exception)}
     * @param supplier to run
     */
    public static <R, E extends Exception> R catchAsRuntimeException(CheckedSupplier<R, E> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            throw convertToRuntime(e);
        }
    }

    /**
     * Run passed runnable and catch exception and translate exception into runtime exception using
     * {@link ExceptionsHelper#convertToRuntime(Exception)}
     * @param runnable to run
     */
    public static void catchAsRuntimeException(CheckedRunnable<Exception> runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw convertToRuntime(e);
        }
    }

    /**
     * Deduplicate the failures by exception message and index.
     */
    public static ShardOperationFailedException[] groupBy(ShardOperationFailedException[] failures) {
        List<ShardOperationFailedException> uniqueFailures = new ArrayList<>();
        Set<GroupBy> reasons = new HashSet<>();
        for (ShardOperationFailedException failure : failures) {
            GroupBy reason = new GroupBy(failure);
            if (reasons.contains(reason) == false) {
                reasons.add(reason);
                uniqueFailures.add(failure);
            }
        }
        return uniqueFailures.toArray(new ShardOperationFailedException[0]);
    }

    private static class GroupBy {
        final String reason;
        final String index;
        final Class<? extends Throwable> causeType;

        GroupBy(ShardOperationFailedException failure) {
            Throwable cause = failure.getCause();
            // the index name from the failure contains the cluster alias when using CCS. Ideally failures should be grouped by
            // index name and cluster alias. That's why the failure index name has the precedence over the one coming from the cause,
            // which does not include the cluster alias.
            String indexName = failure.index();
            if (indexName == null) {
                if (cause instanceof OpenSearchException) {
                    final Index index = ((OpenSearchException) cause).getIndex();
                    if (index != null) {
                        indexName = index.getName();
                    }
                }
            }
            this.index = indexName;
            this.reason = cause.getMessage();
            this.causeType = cause.getClass();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupBy groupBy = (GroupBy) o;
            return Objects.equals(reason, groupBy.reason)
                && Objects.equals(index, groupBy.index)
                && Objects.equals(causeType, groupBy.causeType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, index, causeType);
        }
    }
}
