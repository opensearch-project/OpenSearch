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

package org.opensearch.painless.spi;

import java.util.List;

/**
 * Field represents the equivalent of a Java field available as an allowlisted class field
 * within Painless. Fields for Painless classes may be accessed exactly as fields for Java classes
 * are using the '.' operator on an existing class variable/field.
 */
public class AllowlistField extends WhitelistField {
    /**
     * Standard constructor.  All values must be not {@code null}.
     */
    public AllowlistField(String origin, String fieldName, String canonicalTypeNameParameter, List<Object> painlessAnnotations) {
        super(origin, fieldName, canonicalTypeNameParameter, painlessAnnotations);
    }
}
