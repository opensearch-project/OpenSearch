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
 * An example of a class to be allowlisted for use by painless scripts
 * <p>
 * Each of the members and methods below are allowlisted for use in search scripts.
 * See <a href="file:example_allowlist.txt">example_allowlist.txt</a>.
 */
public class ExampleAllowlistedClass {

    /**
     * An example constant.
     */
    public static final int CONSTANT = 42;

    /**
     * An example public member.
     */
    public int publicMember;

    private int privateMember;

    /**
     * Construct this example class with the given public and private members.
     *
     * @param publicMember The value for the public member.
     * @param privateMember The value for the private member.
     */
    public ExampleAllowlistedClass(int publicMember, int privateMember) {
        this.publicMember = publicMember;
        this.privateMember = privateMember;
    }

    /**
     * Get the value of the private member.
     *
     * @return the value of the private member.
     */
    public int getPrivateMemberAccessor() {
        return this.privateMember;
    }

    /**
     * Set the value of the private member.
     *
     * @param privateMember the value to set.
     */
    public void setPrivateMemberAccessor(int privateMember) {
        this.privateMember = privateMember;
    }

    /**
     * An example static method. You may be shocked, but it does nothing.
     */
    public static void staticMethod() {
        // electricity
    }

    /**
     * An example static augmentation method that takes the object to operate on as the first argument to a static method.
     *
     * @param x The String to be operated on.
     * @return an integer parsed from the input.
     */
    public static int toInt(String x) {
        return Integer.parseInt(x);
    }

    /**
     * An example method to attach annotations in allowlist.
     */
    public void annotate() {
        // some logic here
    }
}
