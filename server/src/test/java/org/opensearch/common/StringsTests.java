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

package org.opensearch.common;

import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

public class StringsTests extends OpenSearchTestCase {

    public void testToStringToXContent() {
        final ToXContent toXContent;
        final boolean error;
        if (randomBoolean()) {
            if (randomBoolean()) {
                error = false;
                toXContent = (builder, params) -> builder.field("ok", "here").field("catastrophe", "");
            } else {
                error = true;
                toXContent = (builder, params) -> builder.startObject().field("ok", "here").field("catastrophe", "").endObject();
            }
        } else {
            if (randomBoolean()) {
                error = false;
                toXContent = (ToXContentObject) (builder, params) -> builder.startObject()
                    .field("ok", "here")
                    .field("catastrophe", "")
                    .endObject();
            } else {
                error = true;
                toXContent = (ToXContentObject) (builder, params) -> builder.field("ok", "here").field("catastrophe", "");
            }
        }

        String toString = Strings.toString(MediaTypeRegistry.JSON, toXContent);
        if (error) {
            assertThat(toString, containsString("\"error\":\"error building toString out of XContent:"));
            assertThat(toString, containsString("\"stack_trace\":"));
        } else {
            assertThat(toString, containsString("\"ok\":\"here\""));
            assertThat(toString, containsString("\"catastrophe\":\"\""));
        }
    }

    public void testToStringToXContentWithOrWithoutParams() {
        ToXContent toXContent = (builder, params) -> builder.field("color_from_param", params.param("color", "red"));
        // Rely on the default value of "color" param when params are not passed
        assertThat(Strings.toString(MediaTypeRegistry.JSON, toXContent), containsString("\"color_from_param\":\"red\""));
        // Pass "color" param explicitly
        assertThat(
            Strings.toString(MediaTypeRegistry.JSON, toXContent, new ToXContent.MapParams(Collections.singletonMap("color", "blue"))),
            containsString("\"color_from_param\":\"blue\"")
        );
    }
}
