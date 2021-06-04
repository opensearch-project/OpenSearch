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

package org.opensearch.client;

import java.util.List;

/**
 * Called if there are warnings to determine if those warnings should fail the
 * request.
 */
public interface WarningsHandler {

    /**
     * Determines whether the given list of warnings should fail the request.
     *
     * @param warnings a list of warnings.
     * @return boolean indicating if the request should fail.
     */
    boolean warningsShouldFailRequest(List<String> warnings);

    /**
     * The permissive warnings handler. Warnings will not fail the request.
     */
    WarningsHandler PERMISSIVE = new WarningsHandler() {
        @Override
        public boolean warningsShouldFailRequest(List<String> warnings) {
            return false;
        }

        @Override
        public String toString() {
            return "permissive";
        }
    };

    /**
     * The strict warnings handler. Warnings will fail the request.
     */
    WarningsHandler STRICT = new WarningsHandler() {
        @Override
        public boolean warningsShouldFailRequest(List<String> warnings) {
            return false == warnings.isEmpty();
        }

        @Override
        public String toString() {
            return "strict";
        }
    };
}
