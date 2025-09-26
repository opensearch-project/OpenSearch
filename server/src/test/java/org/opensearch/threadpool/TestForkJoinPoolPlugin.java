/// *
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// */
//
// package org.opensearch.threadpool;
//
// import org.opensearch.common.settings.Settings;
// import org.opensearch.plugins.Plugin;
//
// import java.util.List;
//
/// **
// * Plugin to register a ForkJoinPool-based thread pool for testing.
// */
// public class TestForkJoinPoolPlugin extends Plugin {
// @Override
// public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
// int parallelism = 2; // Set as needed for your test environment
// return List.of(new ForkJoinPoolExecutorBuilder("jvector", parallelism));
// }
// }
