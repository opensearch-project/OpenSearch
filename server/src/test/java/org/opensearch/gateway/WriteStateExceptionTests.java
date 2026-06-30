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

package org.opensearch.gateway;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOError;
import java.io.UncheckedIOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class WriteStateExceptionTests extends OpenSearchTestCase {

    public void testDirtyFlag() {
        boolean dirty = randomBoolean();
        WriteStateException ex = new WriteStateException(dirty, "test", null);
        assertThat(ex.isDirty(), equalTo(dirty));
    }

    public void testNonDirtyRethrow() {
        WriteStateException ex = new WriteStateException(false, "test", null);
        UncheckedIOException ex2 = expectThrows(UncheckedIOException.class, () -> ex.rethrowAsErrorOrUncheckedException());
        assertThat(ex2.getCause(), instanceOf(WriteStateException.class));
    }

    public void testDirtyRethrow() {
        WriteStateException ex = new WriteStateException(true, "test", null);
        IOError err = expectThrows(IOError.class, () -> ex.rethrowAsErrorOrUncheckedException());
        assertThat(err.getCause(), instanceOf(WriteStateException.class));
    }
}
