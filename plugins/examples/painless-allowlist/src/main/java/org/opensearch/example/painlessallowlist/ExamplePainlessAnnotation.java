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

package org.opensearch.example.painlessallowlist;

/**
 * An example of an annotation to be allowlisted for use by painless scripts
 * <p>
 * The annotation below is allowlisted for use in search scripts.
 * See <a href="file:example_allowlist.txt">example_allowlist.txt</a>.
 */
public class ExamplePainlessAnnotation {

    /**
     * The annotation name.
     */
    public static final String NAME = "example_annotation";

    /**
     * The category field of the annotation.
     */
    public int category;
    /**
     * The message field of the annotation.
     */
    public String message;

    /**
     * Create this annotation with the given category and message.
     *
     * @param category The category of the annotation..
     * @param message The message for the annotation.
     */
    public ExamplePainlessAnnotation(int category, String message) {
        this.category = category;
        this.message = message;
    }

    /**
     * Gets the category.
     *
     * @return the category.
     */
    public int getCategory() {
        return category;
    }

    /**
     * Gets the message.
     *
     * @return the message.
     */
    public String getMessage() {
        return message;
    }
}
