/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.settings;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class RegisterSettingsTests extends OpenSearchTestCase {

    public void testRegisterSettingsRequest() throws Exception {
        String uniqueIdStr = "uniqueid1";
        List<Setting<?>> expected = List.of(
            Setting.boolSetting("falseSetting", false, Property.IndexScope, Property.NodeScope),
            Setting.simpleString("fooSetting", "foo", Property.Dynamic),
            Setting.timeSetting("timeSetting", new TimeValue(5, TimeUnit.MILLISECONDS), Property.Dynamic),
            Setting.byteSizeSetting("byteSizeSetting", new ByteSizeValue(10, ByteSizeUnit.KB), Property.Dynamic)
        );
        RegisterSettingsRequest registerSettingsRequest = new RegisterSettingsRequest(uniqueIdStr, expected);

        assertEquals(uniqueIdStr, registerSettingsRequest.getUniqueId());
        List<Setting<?>> settings = registerSettingsRequest.getSettings();
        assertEquals(expected.size(), settings.size());
        assertTrue(settings.containsAll(expected));
        assertTrue(expected.containsAll(settings));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            registerSettingsRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                registerSettingsRequest = new RegisterSettingsRequest(in);

                assertEquals(uniqueIdStr, registerSettingsRequest.getUniqueId());
                settings = registerSettingsRequest.getSettings();
                assertEquals(expected.size(), settings.size());
                assertTrue(settings.containsAll(expected));
                assertTrue(expected.containsAll(settings));
            }
        }
    }
}
