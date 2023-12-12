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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.painless;

import org.opensearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class RegexLimit2Tests extends ScriptTestCase {
    // This regex has backtracking due to .*?
    private static final String PATTERN = "/abc.*?def/";
    private static final String CHAR_SEQUENCE = "'abcdodef'";
    private static final String SPLIT_CHAR_SEQUENCE = "'0-abc-1-def-X-abc-2-def-Y-abc-3-def-Z-abc'";
    private static PainlessScriptEngine SCRIPT_ENGINE;

    @BeforeClass
    public static void beforeClass() {
        Settings settings = Settings.builder().put(CompilerSettings.REGEX_LIMIT_FACTOR.getKey(), 2).build();
        SCRIPT_ENGINE = new PainlessScriptEngine(settings, newDefaultContexts());
    }

    @AfterClass
    public static void afterClass() {
        SCRIPT_ENGINE = null;
    }

    @Override
    protected PainlessScriptEngine getEngine() {
        return SCRIPT_ENGINE;
    }

    public void testRegexInject_Matcher() {
        String[] scripts = new String[] {
            PATTERN + ".matcher(" + CHAR_SEQUENCE + ").matches()",
            "Matcher m = " + PATTERN + ".matcher(" + CHAR_SEQUENCE + "); m.matches()" };
        for (String script : scripts) {
            assertEquals(Boolean.TRUE, exec(script));
        }
    }

    public void testRegexInject_Def_Matcher() {
        String[] scripts = new String[] {
            "def p = " + PATTERN + "; p.matcher(" + CHAR_SEQUENCE + ").matches()",
            "def p = " + PATTERN + "; def m = p.matcher(" + CHAR_SEQUENCE + "); m.matches()" };
        for (String script : scripts) {
            assertEquals(Boolean.TRUE, exec(script));
        }
    }

    public void testMethodRegexInject_Ref_Matcher() {
        String script = "boolean isMatch(Function func) { func.apply("
            + CHAR_SEQUENCE
            + ").matches(); } "
            + "Pattern pattern = "
            + PATTERN
            + ";"
            + "isMatch(pattern::matcher)";
        assertEquals(Boolean.TRUE, exec(script));
    }

    public void testRegexInject_DefMethodRef_Matcher() {
        String script = "boolean isMatch(Function func) { func.apply("
            + CHAR_SEQUENCE
            + ").matches(); } "
            + "def pattern = "
            + PATTERN
            + ";"
            + "isMatch(pattern::matcher)";
        assertEquals(Boolean.TRUE, exec(script));
    }

    public void testRegexInject_SplitLimit() {
        String[] scripts = new String[] {
            PATTERN + ".split(" + SPLIT_CHAR_SEQUENCE + ", 2)",
            "Pattern p = " + PATTERN + "; p.split(" + SPLIT_CHAR_SEQUENCE + ", 2)" };
        for (String script : scripts) {
            assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));
        }
    }

    public void testRegexInject_Def_SplitLimit() {
        String script = "def p = " + PATTERN + "; p.split(" + SPLIT_CHAR_SEQUENCE + ", 2)";
        assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_Ref_SplitLimit() {
        String script = "String[] splitLimit(BiFunction func) { func.apply("
            + SPLIT_CHAR_SEQUENCE
            + ", 2); } "
            + "Pattern pattern = "
            + PATTERN
            + ";"
            + "splitLimit(pattern::split)";
        assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_DefMethodRef_SplitLimit() {
        String script = "String[] splitLimit(BiFunction func) { func.apply("
            + SPLIT_CHAR_SEQUENCE
            + ", 2); } "
            + "def pattern = "
            + PATTERN
            + ";"
            + "splitLimit(pattern::split)";
        assertArrayEquals(new String[] { "0-", "-X-abc-2-def-Y-abc-3-def-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_Split() {
        String[] scripts = new String[] {
            PATTERN + ".split(" + SPLIT_CHAR_SEQUENCE + ")",
            "Pattern p = " + PATTERN + "; p.split(" + SPLIT_CHAR_SEQUENCE + ")" };
        for (String script : scripts) {
            assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
        }
    }

    public void testRegexInject_Def_Split() {
        String script = "def p = " + PATTERN + "; p.split(" + SPLIT_CHAR_SEQUENCE + ")";
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_Ref_Split() {
        String script = "String[] split(Function func) { func.apply("
            + SPLIT_CHAR_SEQUENCE
            + "); } "
            + "Pattern pattern = "
            + PATTERN
            + ";"
            + "split(pattern::split)";
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_DefMethodRef_Split() {
        String script = "String[] split(Function func) { func.apply("
            + SPLIT_CHAR_SEQUENCE
            + "); } "
            + "def pattern = "
            + PATTERN
            + ";"
            + "split(pattern::split)";
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_SplitAsStream() {
        String[] scripts = new String[] {
            PATTERN + ".splitAsStream(" + SPLIT_CHAR_SEQUENCE + ").toArray(String[]::new)",
            "Pattern p = " + PATTERN + "; p.splitAsStream(" + SPLIT_CHAR_SEQUENCE + ").toArray(String[]::new)" };
        for (String script : scripts) {
            assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
        }
    }

    public void testRegexInject_Def_SplitAsStream() {
        String script = "def p = " + PATTERN + "; p.splitAsStream(" + SPLIT_CHAR_SEQUENCE + ").toArray(String[]::new)";
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_Ref_SplitAsStream() {
        String script = "Stream splitStream(Function func) { func.apply("
            + SPLIT_CHAR_SEQUENCE
            + "); } "
            + "Pattern pattern = "
            + PATTERN
            + ";"
            + "splitStream(pattern::splitAsStream).toArray(String[]::new)";
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInject_DefMethodRef_SplitAsStream() {
        String script = "Stream splitStream(Function func) { func.apply("
            + SPLIT_CHAR_SEQUENCE
            + "); } "
            + "def pattern = "
            + PATTERN
            + ";"
            + "splitStream(pattern::splitAsStream).toArray(String[]::new)";
        assertArrayEquals(new String[] { "0-", "-X-", "-Y-", "-Z-abc" }, (String[]) exec(script));
    }

    public void testRegexInjectFindOperator() {
        String script = "if (" + CHAR_SEQUENCE + " =~ " + PATTERN + ") { return 100; } return 200";
        assertEquals(Integer.valueOf(100), (Integer) exec(script));
    }

    public void testRegexInjectMatchOperator() {
        String script = "if (" + CHAR_SEQUENCE + " ==~ " + PATTERN + ") { return 100; } return 200";
        assertEquals(Integer.valueOf(100), (Integer) exec(script));
    }
}
