/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.common.annotation.PublicApi;

/**
 * Generic interface for profiling wrappers that decorate search components with timing instrumentation
 * while preserving access to the original (delegate) component.
 * <p>
 * When search profiling is enabled, OpenSearch wraps scorers, collectors, aggregators, and other components
 * in profiling decorators that are package-private. This interface provides a public contract for plugins
 * to detect and unwrap such wrappers, accessing the underlying component without requiring reflection
 * or knowledge of package-private implementation classes.
 * <p>
 * Example usage in a plugin to unwrap a profiled scorer:
 * <pre>{@code
 * if (scorer instanceof ProfilingWrapper) {
 *     Scorer delegate = ((ProfilingWrapper<Scorer>) scorer).getDelegate();
 *     // access custom scorer methods on the delegate
 * }
 * }</pre>
 *
 * @param <T> the type of the wrapped component (e.g., {@link org.apache.lucene.search.Scorer},
 *            {@link org.apache.lucene.search.Collector},
 *            {@link org.opensearch.search.aggregations.Aggregator})
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public interface ProfilingWrapper<T> {

    /**
     * Returns the underlying delegate component that this profiling wrapper decorates.
     *
     * @return the original component that was wrapped for profiling
     */
    T getDelegate();
}
