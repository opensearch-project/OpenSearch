/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Service Provider Interface (SPI) for extending the gRPC transport server.
 * Extending plugins which want to modify the underlying netty grpc transport should implement an extension point here.
 * Limits dependencies to core gRPC API, guava, and gRPC service libraries.
 * <p>
 * This SPI contains interface(s):
 * </p>
 * <ul>
 *   <li>{@link org.opensearch.transport.grpc.server.spi.GrpcServiceFactory} -
 *      * Enables plugins extending the grpc-transport module to inject their own services into the gRPC server by
 *      * extending {@link org.opensearch.transport.grpc.server.spi.GrpcServiceFactory} interface. Provides some
 *      * additional resources such as OpenSearch client, settings, thread pool, enabling more complex service
 *      * implementations which can execute requests on the OpenSearch server.
 *  </li>
 * </ul>
 * <p>
 * The SPI mechanism leverages OpenSearch's {@code ExtensiblePlugin} framework. Plugins must:
 * <ul>
 *   <li>Implement {@link org.opensearch.transport.grpc.server.spi.GrpcServiceFactory}</li>
 *   <li>Create a {@code META-INF/services} file listing their converter implementation</li>
 *   <li>Declare {@code transport-grpc} in their plugin descriptor's {@code extended.plugins} list</li>
 * </ul>
 *
 * @since 3.3.0
 */
package org.opensearch.transport.grpc.server.spi;
