/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class CommunityIdProcessorFactoryTests extends OpenSearchTestCase {
    private CommunityIdProcessor.Factory factory;

    @Before
    public void init() {
        factory = new CommunityIdProcessor.Factory();
    }

    public void testCreate() throws Exception {
        boolean ignoreMissing = randomBoolean();
        int seed = randomIntBetween(0, 65535);
        Map<String, Object> config = new HashMap<>();
        config.put("source_ip_field", "source_ip");
        config.put("source_port_field", "source_port");
        config.put("destination_ip_field", "destination_ip");
        config.put("destination_port_field", "destination_port");
        config.put("iana_protocol_number_field", "iana_protocol_number");
        config.put("protocol_field", "protocol");
        config.put("icmp_type_field", "icmp_type");
        config.put("icmp_code_field", "icmp_code");
        config.put("seed", seed);
        config.put("target_field", "community_id_hash");
        config.put("ignore_missing", ignoreMissing);
        String processorTag = randomAlphaOfLength(10);
        CommunityIdProcessor communityIDProcessor = factory.create(null, processorTag, null, config);
        assertThat(communityIDProcessor.getTag(), equalTo(processorTag));
        assertThat(communityIDProcessor.getSourceIPField(), equalTo("source_ip"));
        assertThat(communityIDProcessor.getSourcePortField(), equalTo("source_port"));
        assertThat(communityIDProcessor.getDestinationIPField(), equalTo("destination_ip"));
        assertThat(communityIDProcessor.getDestinationPortField(), equalTo("destination_port"));
        assertThat(communityIDProcessor.getIANAProtocolNumberField(), equalTo("iana_protocol_number"));
        assertThat(communityIDProcessor.getProtocolField(), equalTo("protocol"));
        assertThat(communityIDProcessor.getIcmpTypeField(), equalTo("icmp_type"));
        assertThat(communityIDProcessor.getIcmpCodeField(), equalTo("icmp_code"));
        assertThat(communityIDProcessor.getSeed(), equalTo(seed));
        assertThat(communityIDProcessor.getTargetField(), equalTo("community_id_hash"));
        assertThat(communityIDProcessor.isIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testCreateWithSourceIPField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[source_ip_field] required property is missing"));
        }

        config.put("source_ip_field", null);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[source_ip_field] required property is missing"));
        }
    }

    public void testCreateWithDestinationIPField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("source_ip_field", "source_ip");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[destination_ip_field] required property is missing"));
        }

        config.put("source_ip_field", "source_ip");
        config.put("destination_ip_field", null);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[destination_ip_field] required property is missing"));
        }
    }

    public void testInvalidSeed() throws Exception {
        Map<String, Object> config = new HashMap<>();
        int seed;
        if (randomBoolean()) {
            seed = -1;
        } else {
            seed = 65536;
        }
        config.put("source_ip_field", "source_ip");
        config.put("destination_ip_field", "destination_ip");
        config.put("seed", seed);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchException e) {
            assertThat(e.getMessage(), equalTo("[seed] seed must be between 0 and 65535"));
        }
    }

}
