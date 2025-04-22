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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.client.OriginSettingClient;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.secure_sm.ThreadContextPermission;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskThreadContextStatePropagator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_COUNT;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE;

/**
 * A ThreadContext is a map of string headers and a transient map of keyed objects that are associated with
 * a thread. It allows to store and retrieve header information across method calls, network calls as well as threads spawned from a
 * thread that has a {@link ThreadContext} associated with. Threads spawned from a {@link org.opensearch.threadpool.ThreadPool}
 * have out of the box support for {@link ThreadContext} and all threads spawned will inherit the {@link ThreadContext} from the thread
 * that it is forking from.". Network calls will also preserve the senders headers automatically.
 * <p>
 * Consumers of ThreadContext usually don't need to interact with adding or stashing contexts. Every opensearch thread is managed by
 * a thread pool or executor being responsible for stashing and restoring the threads context. For instance if a network request is
 * received, all headers are deserialized from the network and directly added as the headers of the threads {@link ThreadContext}
 * (see {@link #readHeaders(StreamInput)}. In order to not modify the context that is currently active on this thread the network code
 * uses a try/with pattern to stash it's current context, read headers into a fresh one and once the request is handled or a handler thread
 * is forked (which in turn inherits the context) it restores the previous context. For instance:
 * </p>
 * <pre>
 *     // current context is stashed and replaced with a default context
 *     try (StoredContext context = threadContext.stashContext()) {
 *         threadContext.readHeaders(in); // read headers into current context
 *         if (fork) {
 *             threadPool.execute(() -&gt; request.handle()); // inherits context
 *         } else {
 *             request.handle();
 *         }
 *     }
 *     // previous context is restored on StoredContext#close()
 * </pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ThreadContext implements Writeable {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ThreadContext.class);

    public static final String PREFIX = "request.headers";
    public static final Setting<Settings> DEFAULT_HEADERS_SETTING = Setting.groupSetting(PREFIX + ".", Property.NodeScope);

    /**
     * Name for the {@link #stashWithOrigin origin} attribute.
     */
    public static final String ACTION_ORIGIN_TRANSIENT_NAME = "action.origin";

    // thread context permissions

    private static final Permission ACCESS_SYSTEM_THREAD_CONTEXT_PERMISSION = new ThreadContextPermission("markAsSystemContext");
    private static final Permission STASH_AND_MERGE_THREAD_CONTEXT_PERMISSION = new ThreadContextPermission("stashAndMergeHeaders");
    private static final Permission STASH_WITH_ORIGIN_THREAD_CONTEXT_PERMISSION = new ThreadContextPermission("stashWithOrigin");

    private static final Logger logger = LogManager.getLogger(ThreadContext.class);
    private static final ThreadContextStruct DEFAULT_CONTEXT = new ThreadContextStruct();
    private final Map<String, String> defaultHeader;
    private final ThreadLocal<ThreadContextStruct> threadLocal;
    private final int maxWarningHeaderCount;
    private final long maxWarningHeaderSize;
    private final List<ThreadContextStatePropagator> propagators;

    /**
     * Creates a new ThreadContext instance
     * @param settings the settings to read the default request headers from
     */
    public ThreadContext(Settings settings) {
        this.defaultHeader = buildDefaultHeaders(settings);
        this.threadLocal = ThreadLocal.withInitial(() -> DEFAULT_CONTEXT);
        this.maxWarningHeaderCount = SETTING_HTTP_MAX_WARNING_HEADER_COUNT.get(settings);
        this.maxWarningHeaderSize = SETTING_HTTP_MAX_WARNING_HEADER_SIZE.get(settings).getBytes();
        this.propagators = new CopyOnWriteArrayList<>(List.of(new TaskThreadContextStatePropagator()));
    }

    public void registerThreadContextStatePropagator(final ThreadContextStatePropagator propagator) {
        propagators.add(Objects.requireNonNull(propagator));
    }

    public void unregisterThreadContextStatePropagator(final ThreadContextStatePropagator propagator) {
        propagators.remove(Objects.requireNonNull(propagator));
    }

    /**
     * Removes the current context and resets a default context. The removed context can be
     * restored by closing the returned {@link StoredContext}.
     */
    public StoredContext stashContext() {
        final ThreadContextStruct context = threadLocal.get();
        /*
          X-Opaque-ID should be preserved in a threadContext in order to propagate this across threads.
          This is needed so the DeprecationLogger in another thread can see the value of X-Opaque-ID provided by a user.
          Otherwise when context is stash, it should be empty.
         */

        ThreadContextStruct threadContextStruct = DEFAULT_CONTEXT.putPersistent(context.persistentHeaders);

        if (context.requestHeaders.containsKey(Task.X_OPAQUE_ID)) {
            threadContextStruct = threadContextStruct.putHeaders(
                MapBuilder.<String, String>newMapBuilder()
                    .put(Task.X_OPAQUE_ID, context.requestHeaders.get(Task.X_OPAQUE_ID))
                    .immutableMap()
            );
        }

        final Map<String, Object> transientHeaders = propagateTransients(context.transientHeaders, context.isSystemContext);
        if (!transientHeaders.isEmpty()) {
            threadContextStruct = threadContextStruct.putTransient(transientHeaders);
        }

        threadLocal.set(threadContextStruct);

        return () -> {
            // If the node and thus the threadLocal get closed while this task
            // is still executing, we don't want this runnable to fail with an
            // uncaught exception
            threadLocal.set(context);
        };
    }

    /**
     * Captures the current thread context as writeable, allowing it to be serialized out later
     */
    public Writeable captureAsWriteable() {
        final ThreadContextStruct context = threadLocal.get();
        return out -> {
            final Map<String, String> propagatedHeaders = propagateHeaders(context.transientHeaders, context.isSystemContext);
            context.writeTo(out, defaultHeader, propagatedHeaders);
        };
    }

    /**
     * Removes the current context and resets a default context marked with as
     * originating from the supplied string. The removed context can be
     * restored by closing the returned {@link StoredContext}. Callers should
     * be careful to save the current context before calling this method and
     * restore it any listeners, likely with
     * {@link ContextPreservingActionListener}. Use {@link OriginSettingClient}
     * which can be used to do this automatically.
     * <p>
     * Without security the origin is ignored, but security uses it to authorize
     * actions that are made up of many sub-actions. These actions call
     * {@link #stashWithOrigin} before performing on behalf of a user that
     * should be allowed even if the user doesn't have permission to perform
     * those actions on their own.
     * <p>
     * For example, a user might not have permission to GET from the tasks index
     * but the tasks API will perform a get on their behalf using this method
     * if it can't find the task in memory.
     *
     * Usage of stashWithOrigin is guarded by a ThreadContextPermission. In order to use
     * stashWithOrigin, the codebase needs to explicitly be granted permission in the JSM policy file.
     *
     * Add an entry in the grant portion of the policy file like this:
     *
     * permission org.opensearch.secure_sm.ThreadContextPermission "stashWithOrigin";
     */
    public StoredContext stashWithOrigin(String origin) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            try {
                sm.checkPermission(STASH_WITH_ORIGIN_THREAD_CONTEXT_PERMISSION);
            } catch (SecurityException ex) {
                deprecationLogger.deprecate(
                    "stashWithOrigin",
                    "Default access to stashWithOrigin will be removed in a future release. Permission to use stashWithOrigin must be explicitly granted."
                );
            }
        }
        final ThreadContext.StoredContext storedContext = stashContext();
        putTransient(ACTION_ORIGIN_TRANSIENT_NAME, origin);
        return storedContext;
    }

    /**
     * Removes the current context and resets a new context that contains a merge of the current headers and the given headers.
     * The removed context can be restored when closing the returned {@link StoredContext}. The merge strategy is that headers
     * that are already existing are preserved unless they are defaults.
     *
     * Usage of stashAndMergeHeaders is guarded by a ThreadContextPermission. In order to use
     * stashAndMergeHeaders, the codebase needs to explicitly be granted permission in the JSM policy file.
     *
     * Add an entry in the grant portion of the policy file like this:
     *
     * permission org.opensearch.secure_sm.ThreadContextPermission "stashAndMergeHeaders";
     */
    public StoredContext stashAndMergeHeaders(Map<String, String> headers) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            try {
                sm.checkPermission(STASH_AND_MERGE_THREAD_CONTEXT_PERMISSION);
            } catch (SecurityException ex) {
                deprecationLogger.deprecate(
                    "stashAndMergeHeaders",
                    "Default access to stashAndMergeHeaders will be removed in a future release. Permission to use stashAndMergeHeaders must be explicitly granted."
                );
            }
        }
        final ThreadContextStruct context = threadLocal.get();
        Map<String, String> newHeader = new HashMap<>(headers);
        newHeader.putAll(context.requestHeaders);
        threadLocal.set(DEFAULT_CONTEXT.putHeaders(newHeader));
        return () -> threadLocal.set(context);
    }

    /**
     * Just like {@link #stashContext()} but no default context is set.
     * @param preserveResponseHeaders if set to <code>true</code> the response headers of the restore thread will be preserved.
     */
    public StoredContext newStoredContext(boolean preserveResponseHeaders) {
        return newStoredContext(preserveResponseHeaders, Collections.emptyList());
    }

    /**
     * Just like {@link #stashContext()} but no default context is set. Instead, the {@code transientHeadersToClear} argument can be used
     * to clear specific transient headers in the new context. All headers (with the possible exception of {@code responseHeaders}) are
     * restored by closing the returned {@link StoredContext}.
     *
     * @param preserveResponseHeaders if set to <code>true</code> the response headers of the restore thread will be preserved.
     */
    public StoredContext newStoredContext(boolean preserveResponseHeaders, Collection<String> transientHeadersToClear) {
        final ThreadContextStruct originalContext = threadLocal.get();
        final Map<String, Object> newTransientHeaders = new HashMap<>(originalContext.transientHeaders);

        boolean transientHeadersModified = false;
        final Map<String, Object> transientHeaders = propagateTransients(originalContext.transientHeaders, originalContext.isSystemContext);
        if (!transientHeaders.isEmpty()) {
            newTransientHeaders.putAll(transientHeaders);
            transientHeadersModified = true;
        }

        // clear specific transient headers from the current context
        for (String transientHeaderToClear : transientHeadersToClear) {
            if (newTransientHeaders.containsKey(transientHeaderToClear)) {
                newTransientHeaders.remove(transientHeaderToClear);
                transientHeadersModified = true;
            }
        }

        if (transientHeadersModified == true) {
            ThreadContextStruct threadContextStruct = new ThreadContextStruct(
                originalContext.requestHeaders,
                originalContext.responseHeaders,
                newTransientHeaders,
                originalContext.persistentHeaders,
                originalContext.isSystemContext,
                originalContext.warningHeadersSize
            );
            threadLocal.set(threadContextStruct);
        }
        // this is the context when this method returns
        final ThreadContextStruct newContext = threadLocal.get();

        return () -> {
            if (preserveResponseHeaders && threadLocal.get() != newContext) {
                threadLocal.set(originalContext.putResponseHeaders(threadLocal.get().responseHeaders));
            } else {
                threadLocal.set(originalContext);
            }
        };
    }

    /**
     * Returns a supplier that gathers a {@link #newStoredContext(boolean)} and restores it once the
     * returned supplier is invoked. The context returned from the supplier is a stored version of the
     * suppliers callers context that should be restored once the originally gathered context is not needed anymore.
     * For instance this method should be used like this:
     *
     * <pre>
     *     Supplier&lt;ThreadContext.StoredContext&gt; restorable = context.newRestorableContext(true);
     *     new Thread() {
     *         public void run() {
     *             try (ThreadContext.StoredContext ctx = restorable.get()) {
     *                 // execute with the parents context and restore the threads context afterwards
     *             }
     *         }
     *
     *     }.start();
     * </pre>
     *
     * @param preserveResponseHeaders if set to <code>true</code> the response headers of the restore thread will be preserved.
     * @return a restorable context supplier
     */
    public Supplier<StoredContext> newRestorableContext(boolean preserveResponseHeaders) {
        return wrapRestorable(newStoredContext(preserveResponseHeaders));
    }

    /**
     * Same as {@link #newRestorableContext(boolean)} but wraps an existing context to restore.
     * @param storedContext the context to restore
     */
    public Supplier<StoredContext> wrapRestorable(StoredContext storedContext) {
        return () -> {
            StoredContext context = newStoredContext(false);
            storedContext.restore();
            return context;
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final ThreadContextStruct context = threadLocal.get();
        final Map<String, String> propagatedHeaders = propagateHeaders(context.transientHeaders, context.isSystemContext);
        context.writeTo(out, defaultHeader, propagatedHeaders);
    }

    /**
     * Reads the headers from the stream into the current context
     */
    public void readHeaders(StreamInput in) throws IOException {
        setHeaders(readHeadersFromStream(in));
    }

    public void setHeaders(Tuple<Map<String, String>, Map<String, Set<String>>> headerTuple) {
        final Map<String, String> requestHeaders = headerTuple.v1();
        final Map<String, Set<String>> responseHeaders = headerTuple.v2();
        final ThreadContextStruct struct;
        if (requestHeaders.isEmpty() && responseHeaders.isEmpty()) {
            struct = ThreadContextStruct.EMPTY;
        } else {
            struct = new ThreadContextStruct(requestHeaders, responseHeaders, Collections.emptyMap(), Collections.emptyMap(), false);
        }
        threadLocal.set(struct);
    }

    public static Tuple<Map<String, String>, Map<String, Set<String>>> readHeadersFromStream(StreamInput in) throws IOException {
        final Map<String, String> requestHeaders = in.readMap(StreamInput::readString, StreamInput::readString);
        final Map<String, Set<String>> responseHeaders = in.readMap(StreamInput::readString, input -> {
            final int size = input.readVInt();
            if (size == 0) {
                return Collections.emptySet();
            } else if (size == 1) {
                return Collections.singleton(input.readString());
            } else {
                // use a linked hash set to preserve order
                final LinkedHashSet<String> values = new LinkedHashSet<>(size);
                for (int i = 0; i < size; i++) {
                    final String value = input.readString();
                    final boolean added = values.add(value);
                    assert added : value;
                }
                return values;
            }
        });
        return new Tuple<>(requestHeaders, responseHeaders);
    }

    /**
     * Returns the header for the given key or <code>null</code> if not present
     */
    public String getHeader(String key) {
        String value = threadLocal.get().requestHeaders.get(key);
        if (value == null) {
            return defaultHeader.get(key);
        }
        return value;
    }

    /**
     * Returns the persistent header for the given key or <code>null</code> if not present - persistent headers cannot be stashed
     */
    public Object getPersistent(String key) {
        return threadLocal.get().persistentHeaders.get(key);
    }

    /**
     * Returns all of the request headers from the thread's context.<br>
     * <b>Be advised, headers might contain credentials.</b>
     * In order to avoid storing, and erroneously exposing, such headers,
     * it is recommended to instead store security headers that prove
     * the credentials have been verified successfully, and which are
     * internal to the system, in the sense that they cannot be sent
     * by the clients.
     */
    public Map<String, String> getHeaders() {
        HashMap<String, String> map = new HashMap<>(defaultHeader);
        map.putAll(threadLocal.get().requestHeaders);
        return Collections.unmodifiableMap(map);
    }

    /**
     * Returns the request headers, without the default headers
     */
    public Map<String, String> getRequestHeadersOnly() {
        return Collections.unmodifiableMap(new HashMap<>(threadLocal.get().requestHeaders));
    }

    /**
     * Get a copy of all <em>response</em> headers.
     *
     * @return Never {@code null}.
     */
    public Map<String, List<String>> getResponseHeaders() {
        Map<String, Set<String>> responseHeaders = threadLocal.get().responseHeaders;
        HashMap<String, List<String>> map = new HashMap<>(responseHeaders.size());

        for (Map.Entry<String, Set<String>> entry : responseHeaders.entrySet()) {
            map.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }

        return Collections.unmodifiableMap(map);
    }

    /**
     * Copies all header key, value pairs into the current context
     */
    public void copyHeaders(Iterable<Map.Entry<String, String>> headers) {
        threadLocal.set(threadLocal.get().copyHeaders(headers));
    }

    /**
     * Puts a header into the context
     */
    public void putHeader(String key, String value) {
        threadLocal.set(threadLocal.get().putRequest(key, value));
    }

    /**
     * Puts all of the given headers into this context
     */
    public void putHeader(Map<String, String> header) {
        threadLocal.set(threadLocal.get().putHeaders(header));
    }

    /**
     * Puts a persistent header into the context - persistent headers cannot be stashed
     */
    public void putPersistent(String key, Object value) {
        threadLocal.set(threadLocal.get().putPersistent(key, value));
    }

    /**
     * Puts all of the given headers into this persistent context - persistent headers cannot be stashed
     */
    public void putPersistent(Map<String, Object> persistentHeaders) {
        threadLocal.set(threadLocal.get().putPersistent(persistentHeaders));
    }

    /**
     * Puts a transient header object into this context
     */
    public void putTransient(String key, Object value) {
        threadLocal.set(threadLocal.get().putTransient(key, value));
    }

    /**
     * Returns a transient header object or <code>null</code> if there is no header for the given key
     */
    @SuppressWarnings("unchecked") // (T)object
    public <T> T getTransient(String key) {
        return (T) threadLocal.get().transientHeaders.get(key);
    }

    /**
     * Add the {@code value} for the specified {@code key} Any duplicate {@code value} is ignored.
     *
     * @param key         the header name
     * @param value       the header value
     */
    public void addResponseHeader(final String key, final String value) {
        addResponseHeader(key, value, v -> v);
    }

    /**
     * Update the {@code value} for the specified {@code key}
     *
     * @param key         the header name
     * @param value       the header value
     */
    public void updateResponseHeader(final String key, final String value) {
        updateResponseHeader(key, value, v -> v);
    }

    /**
     * Add the {@code value} for the specified {@code key} with the specified {@code uniqueValue} used for de-duplication. Any duplicate
     * {@code value} after applying {@code uniqueValue} is ignored.
     *
     * @param key         the header name
     * @param value       the header value
     * @param uniqueValue the function that produces de-duplication values
     */
    public void addResponseHeader(final String key, final String value, final Function<String, String> uniqueValue) {
        threadLocal.set(threadLocal.get().putResponse(key, value, uniqueValue, maxWarningHeaderCount, maxWarningHeaderSize, false));
    }

    /**
     * Update the {@code value} for the specified {@code key} with the specified {@code uniqueValue} used for de-duplication. Any duplicate
     * {@code value} after applying {@code uniqueValue} is ignored.
     *
     * @param key         the header name
     * @param value       the header value
     * @param uniqueValue the function that produces de-duplication values
     */
    public void updateResponseHeader(final String key, final String value, final Function<String, String> uniqueValue) {
        threadLocal.set(threadLocal.get().putResponse(key, value, uniqueValue, maxWarningHeaderCount, maxWarningHeaderSize, true));
    }

    /**
     * Remove the {@code value} for the specified {@code key}.
     *
     * @param key         the header name
     */
    public void removeResponseHeader(final String key) {
        threadLocal.get().responseHeaders.remove(key);
    }

    /**
     * Saves the current thread context and wraps command in a Runnable that restores that context before running command. If
     * <code>command</code> has already been passed through this method then it is returned unaltered rather than wrapped twice.
     */
    public Runnable preserveContext(Runnable command) {
        if (command instanceof ContextPreservingAbstractRunnable) {
            return command;
        }
        if (command instanceof ContextPreservingRunnable) {
            return command;
        }
        if (command instanceof AbstractRunnable) {
            return new ContextPreservingAbstractRunnable((AbstractRunnable) command);
        }
        return new ContextPreservingRunnable(command);
    }

    /**
     * Unwraps a command that was previously wrapped by {@link #preserveContext(Runnable)}.
     */
    public Runnable unwrap(Runnable command) {
        if (command instanceof WrappedRunnable) {
            return ((WrappedRunnable) command).unwrap();
        }
        return command;
    }

    /**
     * Returns true if the current context is the default context.
     */
    boolean isDefaultContext() {
        return threadLocal.get() == DEFAULT_CONTEXT;
    }

    /**
     * Marks this thread context as an internal system context. This signals that actions in this context are issued
     * by the system itself rather than by a user action.
     *
     * Usage of markAsSystemContext is guarded by a ThreadContextPermission. In order to use
     * markAsSystemContext, the codebase needs to explicitly be granted permission in the JSM policy file.
     *
     * Add an entry in the grant portion of the policy file like this:
     *
     * permission org.opensearch.secure_sm.ThreadContextPermission "markAsSystemContext";
     */
    public void markAsSystemContext() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            try {
                sm.checkPermission(ACCESS_SYSTEM_THREAD_CONTEXT_PERMISSION);
            } catch (SecurityException ex) {
                deprecationLogger.deprecate(
                    "markAsSystemContext",
                    "Default access to markAsSystemContext will be removed in a future release. Permission to use markAsSystemContext must be explicitly granted."
                );
            }
        }
        threadLocal.set(threadLocal.get().setSystemContext(propagators));
    }

    /**
     * Returns <code>true</code> iff this context is a system context
     */
    public boolean isSystemContext() {
        return threadLocal.get().isSystemContext;
    }

    /**
     * A stored context
     *
     * @opensearch.api
     */
    @FunctionalInterface
    @PublicApi(since = "1.0.0")
    public interface StoredContext extends AutoCloseable {
        @Override
        void close();

        default void restore() {
            close();
        }
    }

    public static Map<String, String> buildDefaultHeaders(Settings settings) {
        Settings headers = DEFAULT_HEADERS_SETTING.get(settings);
        if (headers == null) {
            return Collections.emptyMap();
        } else {
            Map<String, String> defaultHeader = new HashMap<>();
            for (String key : headers.names()) {
                defaultHeader.put(key, headers.get(key));
            }
            return Collections.unmodifiableMap(defaultHeader);
        }
    }

    private Map<String, Object> propagateTransients(Map<String, Object> source, boolean isSystemContext) {
        final Map<String, Object> transients = new HashMap<>();
        propagators.forEach(p -> transients.putAll(p.transients(source, isSystemContext)));
        return transients;
    }

    private Map<String, String> propagateHeaders(Map<String, Object> source, boolean isSystemContext) {
        final Map<String, String> headers = new HashMap<>();
        propagators.forEach(p -> headers.putAll(p.headers(source, isSystemContext)));
        return headers;
    }

    private static final class ThreadContextStruct {

        private static final ThreadContextStruct EMPTY = new ThreadContextStruct(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            false
        );

        private final Map<String, String> requestHeaders;
        private final Map<String, Object> transientHeaders;
        private final Map<String, Set<String>> responseHeaders;
        private final Map<String, Object> persistentHeaders;
        private final boolean isSystemContext;
        // saving current warning headers' size not to recalculate the size with every new warning header
        private final long warningHeadersSize;

        private ThreadContextStruct setSystemContext(final List<ThreadContextStatePropagator> propagators) {
            if (isSystemContext) {
                return this;
            }
            final Map<String, Object> transients = new HashMap<>();
            propagators.forEach(p -> transients.putAll(p.transients(transientHeaders, true)));
            return new ThreadContextStruct(requestHeaders, responseHeaders, transients, persistentHeaders, true);
        }

        private ThreadContextStruct(
            Map<String, String> requestHeaders,
            Map<String, Set<String>> responseHeaders,
            Map<String, Object> transientHeaders,
            Map<String, Object> persistentHeaders,
            boolean isSystemContext
        ) {
            this.requestHeaders = requestHeaders;
            this.responseHeaders = responseHeaders;
            this.transientHeaders = transientHeaders;
            this.persistentHeaders = persistentHeaders;
            this.isSystemContext = isSystemContext;
            this.warningHeadersSize = 0L;
        }

        private ThreadContextStruct(
            Map<String, String> requestHeaders,
            Map<String, Set<String>> responseHeaders,
            Map<String, Object> transientHeaders,
            Map<String, Object> persistentHeaders,
            boolean isSystemContext,
            long warningHeadersSize
        ) {
            this.requestHeaders = requestHeaders;
            this.responseHeaders = responseHeaders;
            this.transientHeaders = transientHeaders;
            this.persistentHeaders = persistentHeaders;
            this.isSystemContext = isSystemContext;
            this.warningHeadersSize = warningHeadersSize;
        }

        /**
         * This represents the default context and it should only ever be called by {@link #DEFAULT_CONTEXT}.
         */
        private ThreadContextStruct() {
            this(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), false);
        }

        private ThreadContextStruct putRequest(String key, String value) {
            Map<String, String> newRequestHeaders = new HashMap<>(this.requestHeaders);
            putSingleHeader(key, value, newRequestHeaders);
            return new ThreadContextStruct(newRequestHeaders, responseHeaders, transientHeaders, persistentHeaders, isSystemContext);
        }

        private static <T> void putSingleHeader(String key, T value, Map<String, T> newHeaders) {
            if (newHeaders.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("value for key [" + key + "] already present");
            }
        }

        private ThreadContextStruct putHeaders(Map<String, String> headers) {
            if (headers.isEmpty()) {
                return this;
            } else {
                final Map<String, String> newHeaders = new HashMap<>(this.requestHeaders);
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    putSingleHeader(entry.getKey(), entry.getValue(), newHeaders);
                }
                return new ThreadContextStruct(newHeaders, responseHeaders, transientHeaders, persistentHeaders, isSystemContext);
            }
        }

        private ThreadContextStruct putPersistent(String key, Object value) {
            Map<String, Object> newPersistentHeaders = new HashMap<>(this.persistentHeaders);
            putSingleHeader(key, value, newPersistentHeaders);
            return new ThreadContextStruct(requestHeaders, responseHeaders, transientHeaders, newPersistentHeaders, isSystemContext);
        }

        private ThreadContextStruct putPersistent(Map<String, Object> headers) {
            if (headers.isEmpty()) {
                return this;
            } else {
                final Map<String, Object> newPersistentHeaders = new HashMap<>(this.persistentHeaders);
                for (Map.Entry<String, Object> entry : headers.entrySet()) {
                    putSingleHeader(entry.getKey(), entry.getValue(), newPersistentHeaders);
                }
                return new ThreadContextStruct(requestHeaders, responseHeaders, transientHeaders, newPersistentHeaders, isSystemContext);
            }
        }

        private ThreadContextStruct putResponseHeaders(Map<String, Set<String>> headers) {
            assert headers != null;
            if (headers.isEmpty()) {
                return this;
            }
            final Map<String, Set<String>> newResponseHeaders = new HashMap<>(this.responseHeaders);
            for (Map.Entry<String, Set<String>> entry : headers.entrySet()) {
                String key = entry.getKey();
                final Set<String> existingValues = newResponseHeaders.get(key);
                if (existingValues != null) {
                    final Set<String> newValues = Stream.concat(entry.getValue().stream(), existingValues.stream())
                        .collect(LINKED_HASH_SET_COLLECTOR);
                    newResponseHeaders.put(key, Collections.unmodifiableSet(newValues));
                } else {
                    newResponseHeaders.put(key, entry.getValue());
                }
            }
            return new ThreadContextStruct(requestHeaders, newResponseHeaders, transientHeaders, persistentHeaders, isSystemContext);
        }

        private ThreadContextStruct putResponse(
            final String key,
            final String value,
            final Function<String, String> uniqueValue,
            final int maxWarningHeaderCount,
            final long maxWarningHeaderSize,
            final boolean replaceExistingKey
        ) {
            assert value != null;
            long newWarningHeaderSize = warningHeadersSize;
            // check if we can add another warning header - if max size within limits
            if (key.equals("Warning") && (maxWarningHeaderSize != -1)) { // if size is NOT unbounded, check its limits
                if (warningHeadersSize > maxWarningHeaderSize) { // if max size has already been reached before
                    logger.warn(
                        "Dropping a warning header, as their total size reached the maximum allowed of ["
                            + maxWarningHeaderSize
                            + "] bytes set in ["
                            + HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE.getKey()
                            + "]!"
                    );
                    return this;
                }
                newWarningHeaderSize += "Warning".getBytes(StandardCharsets.UTF_8).length + value.getBytes(StandardCharsets.UTF_8).length;
                if (newWarningHeaderSize > maxWarningHeaderSize) {
                    logger.warn(
                        "Dropping a warning header, as their total size reached the maximum allowed of ["
                            + maxWarningHeaderSize
                            + "] bytes set in ["
                            + HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE.getKey()
                            + "]!"
                    );
                    return new ThreadContextStruct(
                        requestHeaders,
                        responseHeaders,
                        transientHeaders,
                        persistentHeaders,
                        isSystemContext,
                        newWarningHeaderSize
                    );
                }
            }

            final Map<String, Set<String>> newResponseHeaders;
            final Set<String> existingValues = responseHeaders.get(key);
            if (existingValues != null) {
                if (existingValues.contains(uniqueValue.apply(value))) {
                    return this;
                }
                Set<String> newValues;
                if (replaceExistingKey) {
                    newValues = Stream.of(value).collect(LINKED_HASH_SET_COLLECTOR);
                } else {
                    // preserve insertion order
                    newValues = Stream.concat(existingValues.stream(), Stream.of(value)).collect(LINKED_HASH_SET_COLLECTOR);
                }
                newResponseHeaders = new HashMap<>(responseHeaders);
                newResponseHeaders.put(key, Collections.unmodifiableSet(newValues));
            } else {
                newResponseHeaders = new HashMap<>(responseHeaders);
                newResponseHeaders.put(key, Collections.singleton(value));
            }

            // check if we can add another warning header - if max count within limits
            if ((key.equals("Warning")) && (maxWarningHeaderCount != -1)) { // if count is NOT unbounded, check its limits
                final int warningHeaderCount = newResponseHeaders.containsKey("Warning") ? newResponseHeaders.get("Warning").size() : 0;
                if (warningHeaderCount > maxWarningHeaderCount) {
                    logger.warn(
                        "Dropping a warning header, as their total count reached the maximum allowed of ["
                            + maxWarningHeaderCount
                            + "] set in ["
                            + HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_COUNT.getKey()
                            + "]!"
                    );
                    return this;
                }
            }
            return new ThreadContextStruct(
                requestHeaders,
                newResponseHeaders,
                transientHeaders,
                persistentHeaders,
                isSystemContext,
                newWarningHeaderSize
            );
        }

        private ThreadContextStruct putTransient(Map<String, Object> values) {
            Map<String, Object> newTransient = new HashMap<>(this.transientHeaders);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                putSingleHeader(entry.getKey(), entry.getValue(), newTransient);
            }
            return new ThreadContextStruct(requestHeaders, responseHeaders, newTransient, persistentHeaders, isSystemContext);
        }

        private ThreadContextStruct putTransient(String key, Object value) {
            Map<String, Object> newTransient = new HashMap<>(this.transientHeaders);
            putSingleHeader(key, value, newTransient);
            return new ThreadContextStruct(requestHeaders, responseHeaders, newTransient, persistentHeaders, isSystemContext);
        }

        private ThreadContextStruct copyHeaders(Iterable<Map.Entry<String, String>> headers) {
            Map<String, String> newHeaders = new HashMap<>();
            for (Map.Entry<String, String> header : headers) {
                newHeaders.put(header.getKey(), header.getValue());
            }
            return putHeaders(newHeaders);
        }

        private void writeTo(StreamOutput out, Map<String, String> defaultHeaders, Map<String, String> propagatedHeaders)
            throws IOException {
            final Map<String, String> requestHeaders;
            if (defaultHeaders.isEmpty() && propagatedHeaders.isEmpty()) {
                requestHeaders = this.requestHeaders;
            } else {
                requestHeaders = new HashMap<>(defaultHeaders);
                requestHeaders.putAll(this.requestHeaders);
                requestHeaders.putAll(propagatedHeaders);
            }

            out.writeVInt(requestHeaders.size());
            for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }

            out.writeMap(responseHeaders, StreamOutput::writeString, StreamOutput::writeStringCollection);
        }
    }

    /**
     * Wraps a Runnable to preserve the thread context.
     */
    private class ContextPreservingRunnable implements WrappedRunnable {
        private final Runnable in;
        private final ThreadContext.StoredContext ctx;

        private ContextPreservingRunnable(Runnable in) {
            ctx = newStoredContext(false);
            this.in = in;
        }

        @Override
        public void run() {
            try (ThreadContext.StoredContext ignore = stashContext()) {
                ctx.restore();
                in.run();
            }
        }

        @Override
        public String toString() {
            return in.toString();
        }

        @Override
        public Runnable unwrap() {
            return in;
        }
    }

    /**
     * Wraps an AbstractRunnable to preserve the thread context.
     */
    private class ContextPreservingAbstractRunnable extends AbstractRunnable implements WrappedRunnable {
        private final AbstractRunnable in;
        private final ThreadContext.StoredContext creatorsContext;

        private ThreadContext.StoredContext threadsOriginalContext = null;

        private ContextPreservingAbstractRunnable(AbstractRunnable in) {
            creatorsContext = newStoredContext(false);
            this.in = in;
        }

        @Override
        public boolean isForceExecution() {
            return in.isForceExecution();
        }

        @Override
        public void onAfter() {
            try {
                in.onAfter();
            } finally {
                if (threadsOriginalContext != null) {
                    threadsOriginalContext.restore();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            in.onFailure(e);
        }

        @Override
        public void onRejection(Exception e) {
            in.onRejection(e);
        }

        @Override
        protected void doRun() throws Exception {
            threadsOriginalContext = stashContext();
            creatorsContext.restore();
            in.doRun();
        }

        @Override
        public String toString() {
            return in.toString();
        }

        @Override
        public AbstractRunnable unwrap() {
            return in;
        }
    }

    private static final Collector<String, Set<String>, Set<String>> LINKED_HASH_SET_COLLECTOR = new LinkedHashSetCollector<>();

    /**
     * Collector based on a linked hash set
     *
     * @opensearch.internal
     */
    private static class LinkedHashSetCollector<T> implements Collector<T, Set<T>, Set<T>> {
        @Override
        public Supplier<Set<T>> supplier() {
            return LinkedHashSet::new;
        }

        @Override
        public BiConsumer<Set<T>, T> accumulator() {
            return Set::add;
        }

        @Override
        public BinaryOperator<Set<T>> combiner() {
            return (left, right) -> {
                left.addAll(right);
                return left;
            };
        }

        @Override
        public Function<Set<T>, Set<T>> finisher() {
            return Function.identity();
        }

        private static final Set<Characteristics> CHARACTERISTICS = Collections.unmodifiableSet(
            EnumSet.of(Collector.Characteristics.IDENTITY_FINISH)
        );

        @Override
        public Set<Characteristics> characteristics() {
            return CHARACTERISTICS;
        }
    }

}
