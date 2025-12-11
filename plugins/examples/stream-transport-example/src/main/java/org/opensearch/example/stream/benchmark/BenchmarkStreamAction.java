/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.example.stream.benchmark;

import org.opensearch.action.ActionType;

/**
 * Action for benchmarking stream transport performance
 */
public class BenchmarkStreamAction extends ActionType<BenchmarkStreamResponse> {
    /** Singleton instance */
    public static final BenchmarkStreamAction INSTANCE = new BenchmarkStreamAction();
    /** Action name */
    public static final String NAME = "cluster:admin/benchmark_stream";

    private BenchmarkStreamAction() {
        super(NAME, BenchmarkStreamResponse::new);
    }
}
