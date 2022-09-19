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

public class RegisterCustomSettingsTests extends OpenSearchTestCase {

    public void testRegisterCustomSettingsRequest() throws Exception {
        String uniqueIdStr = "uniqueid1";
        List<Setting<?>> expected = List.of(
            Setting.boolSetting("falseSetting", false, Property.IndexScope, Property.NodeScope),
            Setting.simpleString("fooSetting", "foo", Property.Dynamic),
            Setting.timeSetting("timeSetting", new TimeValue(5, TimeUnit.MILLISECONDS), Property.Dynamic),
            Setting.byteSizeSetting("byteSizeSetting", new ByteSizeValue(10, ByteSizeUnit.KB), Property.Dynamic)
        );
        RegisterCustomSettingsRequest registerCustomSettingsRequest = new RegisterCustomSettingsRequest(uniqueIdStr, expected);

        assertEquals(uniqueIdStr, registerCustomSettingsRequest.getUniqueId());
        List<Setting<?>> settings = registerCustomSettingsRequest.getSettings();
        assertEquals(expected.size(), settings.size());
        assertTrue(settings.containsAll(expected));
        assertTrue(expected.containsAll(settings));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            registerCustomSettingsRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                registerCustomSettingsRequest = new RegisterCustomSettingsRequest(in);

                assertEquals(uniqueIdStr, registerCustomSettingsRequest.getUniqueId());
                settings = registerCustomSettingsRequest.getSettings();
                assertEquals(expected.size(), settings.size());
                assertTrue(settings.containsAll(expected));
                assertTrue(expected.containsAll(settings));
            }
        }
    }
}
