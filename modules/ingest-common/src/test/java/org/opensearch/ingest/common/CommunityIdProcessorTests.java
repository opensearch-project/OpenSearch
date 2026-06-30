/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CommunityIdProcessorTests extends OpenSearchTestCase {

    public void testResolveProtocol() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

        String targetFieldName = randomAlphaOfLength(100);
        boolean ignore_missing = randomBoolean();
        Processor processor = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            null,
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignore_missing
        );
        if (ignore_missing) {
            processor.execute(ingestDocument);
            assertThat(ingestDocument.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "cannot resolve protocol by neither iana protocol number field [iana_protocol_number] nor protocol name field [protocol]",
                IllegalArgumentException.class,
                () -> processor.execute(ingestDocument)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        String protocol = randomAlphaOfLength(10);
        source.put("protocol", protocol);
        IngestDocument ingestDocumentWithProtocol = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithProtocol = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );
        assertThrows(
            "unsupported protocol [" + protocol + "]",
            IllegalArgumentException.class,
            () -> processorWithProtocol.execute(ingestDocumentWithProtocol)
        );

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        int ianaProtocolNumber = randomIntBetween(1000, 10000);
        source.put("iana_protocol_number", ianaProtocolNumber);
        IngestDocument ingestDocumentWithProtocolNumber = RandomDocumentPicks.randomIngestDocument(random(), source);

        Processor processorWithProtocolNumber = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            null,
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );
        assertThrows(
            "unsupported iana protocol number [" + ianaProtocolNumber + "]",
            IllegalArgumentException.class,
            () -> processorWithProtocolNumber.execute(ingestDocumentWithProtocolNumber)
        );
    }

    public void testResolveIPAndPort() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("source_ip", "");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        source.put("protocol", "tcp");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

        String targetFieldName = randomAlphaOfLength(100);
        boolean ignore_missing = randomBoolean();
        Processor processor = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            null,
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignore_missing
        );
        if (ignore_missing) {
            processor.execute(ingestDocument);
            assertThat(ingestDocument.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "ip address in the field [source_ip] is null or empty",
                IllegalArgumentException.class,
                () -> processor.execute(ingestDocument)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        source.put("protocol", "tcp");
        IngestDocument ingestDocumentWithInvalidSourceIP = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithInvalidSourceIP = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );

        assertThrows(
            "ip address in the field [source_ip] is not a valid ipv4/ipv6 address",
            IllegalArgumentException.class,
            () -> processorWithInvalidSourceIP.execute(ingestDocumentWithInvalidSourceIP)
        );

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        source.put("protocol", "tcp");
        ignore_missing = randomBoolean();
        IngestDocument ingestDocumentWithEmptyDestIP = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithEmptyDestIP = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignore_missing
        );
        if (ignore_missing) {
            processorWithEmptyDestIP.execute(ingestDocumentWithEmptyDestIP);
            assertThat(ingestDocumentWithEmptyDestIP.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "ip address in the field [destination_ip] is null or empty",
                IllegalArgumentException.class,
                () -> processorWithEmptyDestIP.execute(ingestDocumentWithEmptyDestIP)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        source.put("protocol", "tcp");
        IngestDocument ingestDocumentWithInvalidDestIP = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithInvalidDestIP = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );
        assertThrows(
            "ip address in the field [destination_ip] is not a valid ipv4/ipv6 address",
            IllegalArgumentException.class,
            () -> processorWithInvalidDestIP.execute(ingestDocumentWithInvalidDestIP)
        );

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        source.put("protocol", "tcp");
        ignore_missing = randomBoolean();
        IngestDocument normalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithEmptySourceIPFieldPath = createCommunityIdProcessor(
            "",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignore_missing
        );
        if (ignore_missing) {
            processorWithEmptySourceIPFieldPath.execute(normalIngestDocument);
            assertThat(normalIngestDocument.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "both source ip field path and destination ip field path cannot be null nor empty",
                IllegalArgumentException.class,
                () -> processorWithEmptySourceIPFieldPath.execute(normalIngestDocument)
            );
        }
        ignore_missing = randomBoolean();
        Processor processorWithEmptyDestIPFieldPath = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignore_missing
        );
        if (ignore_missing) {
            processorWithEmptyDestIPFieldPath.execute(normalIngestDocument);
            assertThat(normalIngestDocument.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "both source ip field path and destination ip field path cannot be null nor empty",
                IllegalArgumentException.class,
                () -> processorWithEmptyDestIPFieldPath.execute(normalIngestDocument)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", null);
        source.put("destination_port", 2000);
        source.put("protocol", "tcp");
        ignore_missing = randomBoolean();
        IngestDocument ingestDocumentWithEmptySourcePort = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithEmptySourcePort = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignore_missing
        );
        if (ignore_missing) {
            processorWithEmptySourcePort.execute(ingestDocumentWithEmptySourcePort);
            assertThat(ingestDocumentWithEmptySourcePort.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "both source port and destination port field path cannot be null nor empty",
                IllegalArgumentException.class,
                () -> processorWithEmptySourcePort.execute(ingestDocumentWithEmptySourcePort)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 65536);
        source.put("destination_port", 2000);
        source.put("protocol", "tcp");
        IngestDocument ingestDocumentWithInvalidSourcePort = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithInvalidSourcePort = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );
        assertThrows(
            "both source port and destination port must be between 0 and 65535, but port in the field path [source_port] is [65536]",
            IllegalArgumentException.class,
            () -> processorWithInvalidSourcePort.execute(ingestDocumentWithInvalidSourcePort)
        );

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", null);
        source.put("protocol", "tcp");
        ignore_missing = randomBoolean();
        IngestDocument ingestDocumentWithEmptyDestPort = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithEmptyDestPort = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignore_missing
        );
        if (ignore_missing) {
            processorWithEmptyDestPort.execute(ingestDocumentWithEmptyDestPort);
            assertThat(ingestDocumentWithEmptyDestPort.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "both source port and destination port cannot be null, but port in the field path [destination_port] is null",
                IllegalArgumentException.class,
                () -> processorWithEmptyDestPort.execute(ingestDocumentWithEmptyDestPort)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", -1);
        source.put("protocol", "tcp");
        IngestDocument ingestDocumentWithInvalidDestPort = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithInvalidDestPort = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );
        assertThrows(
            "both source port and destination port cannot be null, but port in the field path [destination_port] is [-1]",
            IllegalArgumentException.class,
            () -> processorWithInvalidDestPort.execute(ingestDocumentWithInvalidDestPort)
        );
    }

    public void testResolveICMPTypeAndCode() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        int protocolNumber = randomFrom(1, 58);
        source.put("iana_protocol_number", protocolNumber);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);
        String targetFieldName = randomAlphaOfLength(100);
        boolean ignoreMissing = randomBoolean();
        Processor processor = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            null,
            null,
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignoreMissing
        );
        if (ignoreMissing) {
            processor.execute(ingestDocument);
            assertThat(ingestDocument.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "icmp message type field path cannot be null nor empty",
                IllegalArgumentException.class,
                () -> processor.execute(ingestDocument)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        protocolNumber = randomFrom(1, 58);
        source.put("iana_protocol_number", protocolNumber);
        source.put("icmp_type", null);
        IngestDocument ingestDocumentWithNullType = RandomDocumentPicks.randomIngestDocument(random(), source);
        ignoreMissing = randomBoolean();
        Processor processorWithNullType = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            "icmp_type",
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignoreMissing
        );
        if (ignoreMissing) {
            processorWithNullType.execute(ingestDocumentWithNullType);
            assertThat(ingestDocumentWithNullType.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "icmp message type cannot be null nor empty",
                IllegalArgumentException.class,
                () -> processorWithNullType.execute(ingestDocumentWithNullType)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        protocolNumber = randomFrom(1, 58);
        source.put("iana_protocol_number", protocolNumber);
        int icmpType;
        if (randomBoolean()) {
            icmpType = randomIntBetween(256, 1000);
        } else {
            icmpType = randomIntBetween(-100, -1);
        }
        source.put("icmp_type", icmpType);
        IngestDocument ingestDocumentWithInvalidICMPType = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithInvalidICMPType = createCommunityIdProcessor(
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "iana_protocol_number",
            "protocol",
            "icmp_type",
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            false
        );
        assertThrows(
            "invalid icmp message type [" + icmpType + "]",
            IllegalArgumentException.class,
            () -> processorWithInvalidICMPType.execute(ingestDocumentWithInvalidICMPType)
        );

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        protocolNumber = randomFrom(1, 58);
        source.put("iana_protocol_number", protocolNumber);
        if (protocolNumber == 1) {
            icmpType = randomIntBetween(3, 6);
        } else {
            icmpType = randomIntBetween(146, 161);
        }
        source.put("icmp_type", icmpType);
        IngestDocument ingestDocumentWithNoCode = RandomDocumentPicks.randomIngestDocument(random(), source);
        ignoreMissing = randomBoolean();
        Processor processorWithNoCode = createCommunityIdProcessor(
            "source_ip",
            null,
            "destination_ip",
            null,
            "iana_protocol_number",
            "protocol",
            "icmp_type",
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            ignoreMissing
        );
        if (ignoreMissing) {
            processorWithNoCode.execute(ingestDocumentWithNoCode);
            assertThat(ingestDocumentWithNoCode.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "icmp message code field path cannot be null nor empty",
                IllegalArgumentException.class,
                () -> processorWithNoCode.execute(ingestDocumentWithNoCode)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        protocolNumber = randomFrom(1, 58);
        source.put("iana_protocol_number", protocolNumber);
        if (protocolNumber == 1) {
            icmpType = randomIntBetween(3, 6);
        } else {
            icmpType = randomIntBetween(146, 161);
        }
        source.put("icmp_type", icmpType);
        source.put("icmp_code", null);
        IngestDocument ingestDocumentWithNullCode = RandomDocumentPicks.randomIngestDocument(random(), source);
        ignoreMissing = randomBoolean();
        Processor processorWithNullCode = createCommunityIdProcessor(
            "source_ip",
            null,
            "destination_ip",
            null,
            "iana_protocol_number",
            "protocol",
            "icmp_type",
            "icmp_code",
            randomIntBetween(0, 65535),
            targetFieldName,
            ignoreMissing
        );
        if (ignoreMissing) {
            processorWithNullCode.execute(ingestDocumentWithNullCode);
            assertThat(ingestDocumentWithNullCode.hasField(targetFieldName), equalTo(false));
        } else {
            assertThrows(
                "icmp message code cannot be null nor empty",
                IllegalArgumentException.class,
                () -> processorWithNullCode.execute(ingestDocumentWithNullCode)
            );
        }

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        protocolNumber = randomFrom(1, 58);
        source.put("iana_protocol_number", protocolNumber);
        if (protocolNumber == 1) {
            icmpType = randomIntBetween(3, 6);
        } else {
            icmpType = randomIntBetween(146, 161);
        }
        source.put("icmp_type", icmpType);
        int icmpCode;
        if (randomBoolean()) {
            icmpCode = randomIntBetween(256, 1000);
        } else {
            icmpCode = randomIntBetween(-100, -1);
        }
        source.put("icmp_code", icmpCode);
        IngestDocument ingestDocumentWithInvalidCode = RandomDocumentPicks.randomIngestDocument(random(), source);
        Processor processorWithInvalidCode = createCommunityIdProcessor(
            "source_ip",
            null,
            "destination_ip",
            null,
            "iana_protocol_number",
            null,
            "icmp_type",
            "icmp_code",
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );
        assertThrows(
            "invalid icmp message code [" + icmpCode + "]",
            IllegalArgumentException.class,
            () -> processorWithInvalidCode.execute(ingestDocumentWithInvalidCode)
        );
    }

    public void testTransportProtocols() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        source.put("source_port", 1000);
        source.put("destination_port", 2000);
        boolean isProtocolNameSpecified = randomBoolean();
        if (isProtocolNameSpecified) {
            source.put("protocol", randomFrom("tcp", "udp", "sctp"));
        } else {
            source.put("iana_number", randomFrom(6, 17, 132));
        }

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

        String targetFieldName = randomAlphaOfLength(100);
        Processor processor;
        if (isProtocolNameSpecified) {
            processor = createCommunityIdProcessor(
                "source_ip",
                "source_port",
                "destination_ip",
                "destination_port",
                null,
                "protocol",
                null,
                null,
                randomIntBetween(0, 65535),
                targetFieldName,
                randomBoolean()
            );
        } else {
            processor = createCommunityIdProcessor(
                "source_ip",
                "source_port",
                "destination_ip",
                "destination_port",
                "iana_number",
                null,
                null,
                null,
                randomIntBetween(0, 65535),
                targetFieldName,
                randomBoolean()
            );
        }

        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetFieldName), equalTo(true));
        String communityIDHash = ingestDocument.getFieldValue(targetFieldName, String.class);
        assertThat(communityIDHash.startsWith("1:"), equalTo(true));
    }

    public void testICMP() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        boolean isICMP = randomBoolean();
        if (isICMP) {
            source.put("protocol", "icmp");
            source.put("type", randomFrom(0, 8, 9, 10, 13, 15, 17, 18));
        } else {
            source.put("protocol", "ipv6-icmp");
            source.put("type", randomFrom(128, 129, 130, 131, 133, 134, 135, 136, 139, 140, 144, 145));
        }

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

        String targetFieldName = randomAlphaOfLength(100);
        Processor processor = createCommunityIdProcessor(
            "source_ip",
            null,
            "destination_ip",
            null,
            null,
            "protocol",
            "type",
            null,
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );

        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetFieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(targetFieldName, String.class).startsWith("1:"), equalTo(true));

        source = new HashMap<>();
        source.put("source_ip", "1.1.1.1");
        source.put("destination_ip", "2.2.2.2");
        isICMP = randomBoolean();
        if (isICMP) {
            source.put("protocol", "icmp");
            // see https://www.iana.org/assignments/icmp-parameters/icmp-parameters.xhtml#icmp-parameters-codes-5
            source.put("type", randomIntBetween(3, 6));
            source.put("code", 0);
        } else {
            source.put("protocol", "ipv6-icmp");
            // see https://www.iana.org/assignments/icmpv6-parameters/icmpv6-parameters.xhtml#icmpv6-parameters-codes-23
            source.put("type", randomIntBetween(146, 161));
            source.put("code", 0);
        }

        IngestDocument ingestDocumentWithOnewayFlow = RandomDocumentPicks.randomIngestDocument(random(), source);

        targetFieldName = randomAlphaOfLength(100);
        Processor processorWithOnewayFlow = createCommunityIdProcessor(
            "source_ip",
            null,
            "destination_ip",
            null,
            null,
            "protocol",
            "type",
            "code",
            randomIntBetween(0, 65535),
            targetFieldName,
            randomBoolean()
        );

        processorWithOnewayFlow.execute(ingestDocumentWithOnewayFlow);
        assertThat(ingestDocumentWithOnewayFlow.hasField(targetFieldName), equalTo(true));
        assertThat(ingestDocumentWithOnewayFlow.getFieldValue(targetFieldName, String.class).startsWith("1:"), equalTo(true));
    }

    // test that the hash result is consistent with the known value
    public void testHashResult() throws Exception {
        int index = randomIntBetween(0, CommunityIdHashInstance.values().length - 1);
        CommunityIdHashInstance instance = CommunityIdHashInstance.values()[index];
        final boolean isTransportProtocol = instance.name().equals("TCP")
            || instance.name().equals("UDP")
            || instance.name().equals("SCTP");
        Map<String, Object> source = new HashMap<>();
        source.put("source_ip", instance.getSourceIp());
        source.put("destination_ip", instance.getDestIP());
        if (isTransportProtocol) {
            source.put("source_port", instance.getSourcePort());
            source.put("destination_port", instance.getDestPort());
            source.put("iana_number", instance.getProtocolNumber());
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

            String targetFieldName = randomAlphaOfLength(100);
            boolean ignore_missing = randomBoolean();
            Processor processor = createCommunityIdProcessor(
                "source_ip",
                "source_port",
                "destination_ip",
                "destination_port",
                "iana_number",
                null,
                null,
                null,
                0,
                targetFieldName,
                ignore_missing
            );

            processor.execute(ingestDocument);
            assertThat(ingestDocument.hasField(targetFieldName), equalTo(true));
            assertThat(ingestDocument.getFieldValue(targetFieldName, String.class), equalTo(instance.getHash()));

            // test the flow tuple in reversed direction, the hash result should be the same value
            source = new HashMap<>();
            source.put("source_ip", instance.getDestIP());
            source.put("destination_ip", instance.getSourceIp());
            source.put("source_port", instance.getDestPort());
            source.put("destination_port", instance.getSourcePort());
            source.put("iana_number", instance.getProtocolNumber());
            IngestDocument ingestDocumentWithReversedDirection = RandomDocumentPicks.randomIngestDocument(random(), source);

            targetFieldName = randomAlphaOfLength(100);
            Processor processorWithReversedDirection = createCommunityIdProcessor(
                "source_ip",
                "source_port",
                "destination_ip",
                "destination_port",
                "iana_number",
                null,
                null,
                null,
                0,
                targetFieldName,
                randomBoolean()
            );

            processorWithReversedDirection.execute(ingestDocumentWithReversedDirection);
            assertThat(ingestDocumentWithReversedDirection.hasField(targetFieldName), equalTo(true));
            assertThat(ingestDocumentWithReversedDirection.getFieldValue(targetFieldName, String.class), equalTo(instance.getHash()));
        } else {
            source.put("type", instance.getSourcePort());
            source.put("code", instance.getDestPort());
            source.put("iana_number", instance.getProtocolNumber());
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), source);

            String targetFieldName = randomAlphaOfLength(100);
            boolean ignore_missing = randomBoolean();
            Processor processor = createCommunityIdProcessor(
                "source_ip",
                null,
                "destination_ip",
                null,
                "iana_number",
                null,
                "type",
                "code",
                0,
                targetFieldName,
                ignore_missing
            );

            processor.execute(ingestDocument);
            assertThat(ingestDocument.hasField(targetFieldName), equalTo(true));
            assertThat(ingestDocument.getFieldValue(targetFieldName, String.class), equalTo(instance.getHash()));
        }
    }

    private enum CommunityIdHashInstance {
        TCP("66.35.250.204", "128.232.110.120", 6, 80, 34855, "1:LQU9qZlK+B5F3KDmev6m5PMibrg="),
        UDP("8.8.8.8", "192.168.1.52", 17, 53, 54585, "1:d/FP5EW3wiY1vCndhwleRRKHowQ="),
        SCTP("192.168.170.8", "192.168.170.56", 132, 7, 7, "1:MP2EtRCAUIZvTw6MxJHLV7N7JDs="),
        ICMP("192.168.0.89", "192.168.0.1", 1, 8, 0, "1:X0snYXpgwiv9TZtqg64sgzUn6Dk="),
        ICMP_V6("fe80::260:97ff:fe07:69ea", "ff02::1", 58, 134, 0, "1:pkvHqCL88/tg1k4cPigmZXUtL00=");

        private final String sourceIp;
        private final String destIP;
        private final int protocolNumber;
        private final int sourcePort;
        private final int destPort;
        private final String hash;

        CommunityIdHashInstance(String sourceIp, String destIP, int protocolNumber, int sourcePort, int destPort, String hash) {
            this.sourceIp = sourceIp;
            this.destIP = destIP;
            this.protocolNumber = protocolNumber;
            this.sourcePort = sourcePort;
            this.destPort = destPort;
            this.hash = hash;
        }

        private String getSourceIp() {
            return this.sourceIp;
        }

        private String getDestIP() {
            return this.destIP;
        }

        private int getProtocolNumber() {
            return this.protocolNumber;
        }

        private int getSourcePort() {
            return this.sourcePort;
        }

        private int getDestPort() {
            return this.destPort;
        }

        private String getHash() {
            return this.hash;
        }
    }

    private static Processor createCommunityIdProcessor(
        String sourceIPField,
        String sourcePortField,
        String destinationIPField,
        String destinationPortField,
        String ianaProtocolNumberField,
        String protocolField,
        String icmpTypeField,
        String icmpCodeField,
        int seed,
        String targetField,
        boolean ignoreMissing
    ) {
        return new CommunityIdProcessor(
            randomAlphaOfLength(10),
            null,
            sourceIPField,
            sourcePortField,
            destinationIPField,
            destinationPortField,
            ianaProtocolNumberField,
            protocolField,
            icmpTypeField,
            icmpCodeField,
            seed,
            targetField,
            ignoreMissing
        );
    }
}
