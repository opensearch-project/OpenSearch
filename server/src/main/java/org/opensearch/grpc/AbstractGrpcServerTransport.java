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

package org.opensearch.grpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;

/**
 * Base GrpcServer class
 *
 * @opensearch.internal
 */
public abstract class AbstractGrpcServerTransport extends AbstractLifecycleComponent implements GrpcServerTransport {
    private static final Logger logger = LogManager.getLogger(AbstractGrpcServerTransport.class);
    private static final ActionListener<Void> NO_OP = ActionListener.wrap(() -> {});

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public GrpcInfo info() {
        return null;
    }

    @Override
    public GrpcStats stats() {
        return null;
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }
}
