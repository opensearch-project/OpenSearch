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

package org.opensearch.client.benchmark;

import org.opensearch.client.benchmark.rest.RestClientBenchmark;
import org.opensearch.common.SuppressForbidden;

import java.util.Arrays;

public class BenchmarkMain {
    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public static void main(String[] args) throws Exception {
        String type = args[0];
        AbstractBenchmark<?> benchmark = null;
        if ("rest".equals(type)) {
            benchmark = new RestClientBenchmark();
        } else {
            System.err.println("Unknown client type [" + type + "]");
            System.exit(1);
        }
        benchmark.run(Arrays.copyOfRange(args, 1, args.length));
    }
}
