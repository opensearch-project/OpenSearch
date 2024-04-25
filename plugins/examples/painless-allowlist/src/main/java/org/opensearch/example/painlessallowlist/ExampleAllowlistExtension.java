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

import org.opensearch.painless.spi.Allowlist;
import org.opensearch.painless.spi.AllowlistInstanceBinding;
import org.opensearch.painless.spi.AllowlistLoader;
import org.opensearch.painless.spi.PainlessExtension;
import org.opensearch.painless.spi.annotation.AllowlistAnnotationParser;
import org.opensearch.script.FieldScript;
import org.opensearch.script.ScriptContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An extension of painless which adds an allowlist. */
public class ExampleAllowlistExtension implements PainlessExtension {

    /**
     * Instantiate this class.
     */
    public ExampleAllowlistExtension() {}

    @Override
    public Map<ScriptContext<?>, List<Allowlist>> getContextAllowlists() {
        Map<String, AllowlistAnnotationParser> parsers = new HashMap<>(AllowlistAnnotationParser.BASE_ANNOTATION_PARSERS);
        parsers.put(ExamplePainlessAnnotation.NAME, ExampleAllowlistAnnotationParser.INSTANCE);
        Allowlist classAllowlist = AllowlistLoader.loadFromResourceFiles(ExampleAllowlistExtension.class, parsers, "example_allowlist.txt");

        ExampleAllowlistedInstance eai = new ExampleAllowlistedInstance(1);
        AllowlistInstanceBinding addValue = new AllowlistInstanceBinding(
            "example addValue",
            eai,
            "addValue",
            "int",
            Collections.singletonList("int"),
            Collections.emptyList()
        );
        AllowlistInstanceBinding getValue = new AllowlistInstanceBinding(
            "example getValue",
            eai,
            "getValue",
            "int",
            Collections.emptyList(),
            Collections.emptyList()
        );
        Allowlist instanceAllowlist = new Allowlist(
            eai.getClass().getClassLoader(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Arrays.asList(addValue, getValue)
        );

        return Collections.singletonMap(FieldScript.CONTEXT, Arrays.asList(classAllowlist, instanceAllowlist));
    }
}
