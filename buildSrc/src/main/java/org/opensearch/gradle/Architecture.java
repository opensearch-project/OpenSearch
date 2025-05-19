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
 *         http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.gradle;

public enum Architecture {

    X64,
    ARM64,
    S390X,
    PPC64LE,
    RISCV64;

    public static Architecture current() {
        final String architecture = System.getProperty("os.arch", "");
        switch (architecture) {
            case "amd64":
            case "x86_64":
                return X64;
            case "aarch64":
                return ARM64;
            case "s390x":
                return S390X;
            case "ppc64le":
                return PPC64LE;
            case "riscv64":
                return RISCV64;
            default:
                throw new IllegalArgumentException("can not determine architecture from [" + architecture + "]");
        }
    }

}
