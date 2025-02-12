/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.hash.MessageDigests;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.core.common.Strings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that generating community id flow hash for the network flow tuples, the algorithm is defined in
 * <a href="https://github.com/corelight/community-id-spec">Community ID Flow Hashing</a>.
 */
public final class CommunityIdProcessor extends AbstractProcessor {
    public static final String TYPE = "community_id";
    // the version of the community id flow hashing algorithm
    private static final String COMMUNITY_ID_HASH_VERSION = "1";
    // 0 byte for padding
    private static final byte PADDING_BYTE = 0;
    // the maximum code number for network protocol, ICMP message type and code as defined by IANA
    private static final int IANA_COMMON_MAX_NUMBER = 255;
    // the minimum code number for network protocol, ICMP message type and code as defined by IANA
    private static final int IANA_COMMON_MIN_NUMBER = 0;
    // the minimum seed for generating hash
    private static final int MIN_SEED = 0;
    // the maximum seed for generating hash
    private static final int MAX_SEED = 65535;
    // the minimum port number in transport layer
    private static final int MIN_PORT = 0;
    // the maximum port number in transport layer
    private static final int MAX_PORT = 63335;
    private static final String ICMP_MESSAGE_TYPE = "type";
    private static final String ICMP_MESSAGE_CODE = "code";
    private final String sourceIPField;
    private final String sourcePortField;
    private final String destinationIPField;
    private final String destinationPortField;
    private final String ianaProtocolNumberField;
    private final String protocolField;
    private final String icmpTypeField;
    private final String icmpCodeField;
    private final int seed;
    private final String targetField;
    private final boolean ignoreMissing;

    CommunityIdProcessor(
        String tag,
        String description,
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
        super(tag, description);
        this.sourceIPField = sourceIPField;
        this.sourcePortField = sourcePortField;
        this.destinationIPField = destinationIPField;
        this.destinationPortField = destinationPortField;
        this.ianaProtocolNumberField = ianaProtocolNumberField;
        this.protocolField = protocolField;
        this.icmpTypeField = icmpTypeField;
        this.icmpCodeField = icmpCodeField;
        this.seed = seed;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
    }

    public String getSourceIPField() {
        return sourceIPField;
    }

    public String getSourcePortField() {
        return sourcePortField;
    }

    public String getDestinationIPField() {
        return destinationIPField;
    }

    public String getDestinationPortField() {
        return destinationPortField;
    }

    public String getIANAProtocolNumberField() {
        return ianaProtocolNumberField;
    }

    public String getProtocolField() {
        return protocolField;
    }

    public String getIcmpTypeField() {
        return icmpTypeField;
    }

    public String getIcmpCodeField() {
        return icmpCodeField;
    }

    public int getSeed() {
        return seed;
    }

    public String getTargetField() {
        return targetField;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        // resolve protocol firstly
        Protocol protocol = resolveProtocol(document);
        // exit quietly if protocol cannot be resolved and ignore_missing is true
        if (protocol == null) {
            return document;
        }

        // resolve ip secondly, exit quietly if either source ip or destination ip cannot be resolved and ignore_missing is true
        byte[] sourceIPByteArray = resolveIP(document, sourceIPField);
        if (sourceIPByteArray == null) {
            return document;
        }
        byte[] destIPByteArray = resolveIP(document, destinationIPField);
        if (destIPByteArray == null) {
            return document;
        }
        // source ip and destination ip must have same format, either ipv4 or ipv6
        if (sourceIPByteArray.length != destIPByteArray.length) {
            throw new IllegalArgumentException("source ip and destination ip must have same format");
        }

        // resolve source port and destination port for transport protocols,
        // exit quietly if either source port or destination port is null nor empty
        Integer sourcePort = null;
        Integer destinationPort = null;
        if (protocol.isTransportProtocol()) {
            sourcePort = resolvePort(document, sourcePortField);
            if (sourcePort == null) {
                return document;
            }

            destinationPort = resolvePort(document, destinationPortField);
            if (destinationPort == null) {
                return document;
            }
        }

        // resolve ICMP message type and code, support both ipv4 and ipv6
        // set source port to icmp type, and set dest port to icmp code, so that we can have a generic way to handle
        // all protocols
        boolean isOneway = true;
        final boolean isICMPProtocol = Protocol.ICMP == protocol || Protocol.ICMP_V6 == protocol;
        if (isICMPProtocol) {
            Integer icmpType = resolveICMP(document, icmpTypeField, ICMP_MESSAGE_TYPE);
            if (icmpType == null) {
                return document;
            } else {
                sourcePort = icmpType;
            }

            // for the message types which don't have code, fetch the equivalent code from the pre-defined mapper,
            // and they can be considered to two-way flow
            Byte equivalentCode = Protocol.ICMP.getProtocolCode() == protocol.getProtocolCode()
                ? ICMPType.getEquivalentCode(icmpType.byteValue())
                : ICMPv6Type.getEquivalentCode(icmpType.byteValue());
            if (equivalentCode != null) {
                isOneway = false;
                // for IPv6-ICMP, the pre-defined code is negative byte,
                // we need to convert it to positive integer for later comparison
                destinationPort = Protocol.ICMP.getProtocolCode() == protocol.getProtocolCode()
                    ? Integer.valueOf(equivalentCode)
                    : Byte.toUnsignedInt(equivalentCode);
            } else {
                // get icmp code from the document if we cannot get equivalent code from the pre-defined mapper
                Integer icmpCode = resolveICMP(document, icmpCodeField, ICMP_MESSAGE_CODE);
                if (icmpCode == null) {
                    return document;
                } else {
                    destinationPort = icmpCode;
                }
            }
        }

        assert (sourcePort != null && destinationPort != null);
        boolean isLess = compareIPAndPort(sourceIPByteArray, sourcePort, destIPByteArray, destinationPort);
        // swap ip and port to remove directionality in the flow tuple, smaller ip:port tuple comes first
        // but for ICMP and IPv6-ICMP, if it's a one-way flow, the flow tuple is considered to be ordered
        if (!isLess && (!isICMPProtocol || !isOneway)) {
            byte[] byteArray = sourceIPByteArray;
            sourceIPByteArray = destIPByteArray;
            destIPByteArray = byteArray;

            int tempPort = sourcePort;
            sourcePort = destinationPort;
            destinationPort = tempPort;
        }

        // generate flow hash
        String digest = generateCommunityIDHash(
            protocol.getProtocolCode(),
            sourceIPByteArray,
            destIPByteArray,
            sourcePort,
            destinationPort,
            seed
        );
        document.setFieldValue(targetField, digest);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Resolve network protocol
     * @param document the ingesting document
     * @return the resolved protocol, null if the resolved protocol is null and ignore_missing is true
     * @throws IllegalArgumentException only if ignoreMissing is false and the field is null, empty, invalid,
     * or if the field that is found at the provided path is not of the expected type.
     */
    private Protocol resolveProtocol(IngestDocument document) {
        Protocol protocol = null;
        Integer ianaProtocolNumber = null;
        String protocolName = null;
        if (!Strings.isNullOrEmpty(ianaProtocolNumberField)) {
            ianaProtocolNumber = document.getFieldValue(ianaProtocolNumberField, Integer.class, true);
        }
        if (!Strings.isNullOrEmpty(protocolField)) {
            protocolName = document.getFieldValue(protocolField, String.class, true);
        }
        // if iana protocol number is not specified, then resolve protocol name
        if (ianaProtocolNumber != null) {
            if (ianaProtocolNumber >= IANA_COMMON_MIN_NUMBER
                && ianaProtocolNumber <= IANA_COMMON_MAX_NUMBER
                && Protocol.protocolCodeMap.containsKey(ianaProtocolNumber.byteValue())) {
                protocol = Protocol.protocolCodeMap.get(ianaProtocolNumber.byteValue());
            } else {
                throw new IllegalArgumentException("unsupported iana protocol number [" + ianaProtocolNumber + "]");
            }
        } else if (protocolName != null) {
            Protocol protocolFromName = Protocol.fromProtocolName(protocolName);
            if (protocolFromName != null) {
                protocol = protocolFromName;
            } else {
                throw new IllegalArgumentException("unsupported protocol [" + protocolName + "]");
            }
        }

        // return null if protocol cannot be resolved and ignore_missing is true
        if (protocol == null) {
            if (ignoreMissing) {
                return null;
            } else {
                throw new IllegalArgumentException(
                    "cannot resolve protocol by neither iana protocol number field ["
                        + ianaProtocolNumberField
                        + "] nor protocol name field ["
                        + protocolField
                        + "]"
                );
            }
        }
        return protocol;
    }

    /**
     * Resolve ip address
     * @param document the ingesting document
     * @param fieldName the ip field to be resolved
     * @return the byte array of the resolved ip
     * @throws IllegalArgumentException only if ignoreMissing is false and the field is null, empty, invalid,
     * or if the field that is found at the provided path is not of the expected type.
     */
    private byte[] resolveIP(IngestDocument document, String fieldName) {
        if (Strings.isNullOrEmpty(fieldName)) {
            if (ignoreMissing) {
                return null;
            } else {
                throw new IllegalArgumentException("both source ip field path and destination ip field path cannot be null nor empty");
            }
        }

        String ipAddress = document.getFieldValue(fieldName, String.class, true);
        if (Strings.isNullOrEmpty(ipAddress)) {
            if (ignoreMissing) {
                return null;
            } else {
                throw new IllegalArgumentException("ip address in the field [" + fieldName + "] is null or empty");
            }
        }

        byte[] byteArray = InetAddresses.ipStringToBytes(ipAddress);
        if (byteArray == null) {
            throw new IllegalArgumentException(
                "ip address [" + ipAddress + "] in the field [" + fieldName + "] is not a valid ipv4/ipv6 address"
            );
        } else {
            return byteArray;
        }
    }

    /**
     * Resolve port for transport protocols
     * @param document the ingesting document
     * @param fieldName the port field to be resolved
     * @return the resolved port number, null if the resolved port is null and ignoreMissing is true
     * @throws IllegalArgumentException only if ignoreMissing is false and the field is null, empty, invalid,
     * or if the field that is found at the provided path is not of the expected type.
     */
    private Integer resolvePort(IngestDocument document, String fieldName) {
        Integer port;
        if (Strings.isNullOrEmpty(fieldName)) {
            if (ignoreMissing) {
                return null;
            } else {
                throw new IllegalArgumentException("both source port and destination port field path cannot be null nor empty");
            }
        } else {
            port = document.getFieldValue(fieldName, Integer.class, true);
        }

        if (port == null) {
            if (ignoreMissing) {
                return null;
            } else {
                throw new IllegalArgumentException(
                    "both source port and destination port cannot be null, but port in the field path [" + fieldName + "] is null"
                );
            }
        } else if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException(
                "both source port and destination port must be between 0 and 65535, but port in the field path ["
                    + fieldName
                    + "] is ["
                    + port
                    + "]"
            );
        }
        return port;
    }

    /**
     * Resolve ICMP's message type and code field
     * @param document the ingesting document
     * @param fieldName name of the type or the code field
     * @param fieldType type or code
     * @return the resolved value of the specified field, return null if ignore_missing if true and the field doesn't exist or is null,
     * @throws IllegalArgumentException only if ignoreMissing is false and the field is null, empty, invalid,
     * or if the field that is found at the provided path is not of the expected type.
     */
    private Integer resolveICMP(IngestDocument document, String fieldName, String fieldType) {
        if (Strings.isNullOrEmpty(fieldName)) {
            if (ignoreMissing) {
                return null;
            } else {
                throw new IllegalArgumentException("icmp message " + fieldType + " field path cannot be null nor empty");
            }
        }
        Integer fieldValue = document.getFieldValue(fieldName, Integer.class, true);
        if (fieldValue == null) {
            if (ignoreMissing) {
                return null;
            } else {
                throw new IllegalArgumentException("icmp message " + fieldType + " cannot be null");
            }
        } else if (fieldValue < IANA_COMMON_MIN_NUMBER || fieldValue > IANA_COMMON_MAX_NUMBER) {
            throw new IllegalArgumentException("invalid icmp message " + fieldType + " [" + fieldValue + "]");
        } else {
            return fieldValue;
        }
    }

    /**
     *
     * @param protocolCode byte of the protocol number
     * @param sourceIPByteArray bytes of the source ip in the network flow tuple
     * @param destIPByteArray bytes of the destination ip in the network flow tuple
     * @param sourcePort source port in the network flow tuple
     * @param destinationPort destination port in the network flow tuple
     * @param seed seed for generating hash
     * @return the generated hash value, use SHA-1
     */
    private String generateCommunityIDHash(
        byte protocolCode,
        byte[] sourceIPByteArray,
        byte[] destIPByteArray,
        Integer sourcePort,
        Integer destinationPort,
        int seed
    ) {
        MessageDigest messageDigest = MessageDigests.sha1();
        messageDigest.update(intToTwoByteArray(seed));
        messageDigest.update(sourceIPByteArray);
        messageDigest.update(destIPByteArray);
        messageDigest.update(protocolCode);
        messageDigest.update(PADDING_BYTE);
        messageDigest.update(intToTwoByteArray(sourcePort));
        messageDigest.update(intToTwoByteArray(destinationPort));

        return COMMUNITY_ID_HASH_VERSION + ":" + Base64.getEncoder().encodeToString(messageDigest.digest());
    }

    /**
     * Convert an integer to two byte array
     * @param val the integer which will be consumed to produce a two byte array
     * @return the two byte array
     */
    private byte[] intToTwoByteArray(Integer val) {
        byte[] byteArray = new byte[2];
        byteArray[0] = Integer.valueOf(val >>> 8).byteValue();
        byteArray[1] = val.byteValue();
        return byteArray;
    }

    /**
     * Compare the ip and port, return true if the flow tuple is ordered
     * @param sourceIPByteArray bytes of the source ip in the network flow tuple
     * @param destIPByteArray bytes of the destination ip in the network flow tuple
     * @param sourcePort source port in the network flow tuple
     * @param destinationPort destination port in the network flow tuple
     * @return true if sourceIP is less than destinationIP or sourceIP equals to destinationIP
     * but sourcePort is less than destinationPort
     */
    private boolean compareIPAndPort(byte[] sourceIPByteArray, int sourcePort, byte[] destIPByteArray, int destinationPort) {
        int compareResult = compareByteArray(sourceIPByteArray, destIPByteArray);
        return compareResult < 0 || compareResult == 0 && sourcePort < destinationPort;
    }

    /**
     * Compare two byte array which have same length
     * @param byteArray1 the first byte array to compare
     * @param byteArray2 the second byte array to compare
     * @return 0 if each byte in both two arrays are same, a value less than 0 if byte in the first array is less than
     * the byte at the same index, a value greater than 0 if byte in the first array is greater than the byte at the same index
     */
    private int compareByteArray(byte[] byteArray1, byte[] byteArray2) {
        assert (byteArray1.length == byteArray2.length);
        int i = 0;
        int j = 0;
        while (i < byteArray1.length && j < byteArray2.length) {
            int isLess = Byte.compareUnsigned(byteArray1[i], byteArray2[j]);
            if (isLess == 0) {
                i++;
                j++;
            } else {
                return isLess;
            }
        }
        return 0;
    }

    /**
     * Mapping ICMP's message type and code into a port-like notion for ordering the request or response
     */
    enum ICMPType {
        ECHO_REPLY((byte) 0, (byte) 8),
        ECHO((byte) 8, (byte) 0),
        RTR_ADVERT((byte) 9, (byte) 10),
        RTR_SOLICIT((byte) 10, (byte) 9),
        TSTAMP((byte) 13, (byte) 14),
        TSTAMP_REPLY((byte) 14, (byte) 13),
        INFO((byte) 15, (byte) 16),
        INFO_REPLY((byte) 16, (byte) 15),
        MASK((byte) 17, (byte) 18),
        MASK_REPLY((byte) 18, (byte) 17);

        private final byte type;
        private final byte code;

        ICMPType(byte type, byte code) {
            this.type = type;
            this.code = code;
        }

        private static final Map<Byte, Byte> ICMPTypeMapper = Arrays.stream(values()).collect(Collectors.toMap(t -> t.type, t -> t.code));

        /**
         * Takes the message type of ICMP and derives equivalent message code
         * @param type the message type of ICMP
         * @return the equivalent message code
         */
        public static Byte getEquivalentCode(int type) {
            return ICMPTypeMapper.get(Integer.valueOf(type).byteValue());
        }
    }

    /**
     * Mapping IPv6-ICMP's message type and code into a port-like notion for ordering the request or response
     */
    enum ICMPv6Type {
        ECHO_REQUEST((byte) 128, (byte) 129),
        ECHO_REPLY((byte) 129, (byte) 128),
        MLD_LISTENER_QUERY((byte) 130, (byte) 131),
        MLD_LISTENER_REPORT((byte) 131, (byte) 130),
        ND_ROUTER_SOLICIT((byte) 133, (byte) 134),
        ND_ROUTER_ADVERT((byte) 134, (byte) 133),
        ND_NEIGHBOR_SOLICIT((byte) 135, (byte) 136),
        ND_NEIGHBOR_ADVERT((byte) 136, (byte) 135),
        WRU_REQUEST((byte) 139, (byte) 140),
        WRU_REPLY((byte) 140, (byte) 139),
        HAAD_REQUEST((byte) 144, (byte) 145),
        HAAD_REPLY((byte) 145, (byte) 144);

        private final byte type;
        private final byte code;

        ICMPv6Type(byte type, byte code) {
            this.type = type;
            this.code = code;
        }

        private static final Map<Byte, Byte> ICMPTypeMapper = Arrays.stream(values()).collect(Collectors.toMap(t -> t.type, t -> t.code));

        /**
         * Takes the message type of IPv6-ICMP and derives equivalent message code
         * @param type the message type of IPv6-ICMP
         * @return the equivalent message code
         */
        public static Byte getEquivalentCode(int type) {
            return ICMPTypeMapper.get(Integer.valueOf(type).byteValue());
        }
    }

    /**
     * An enumeration of the supported network protocols
     */
    enum Protocol {
        ICMP((byte) 1, false),
        TCP((byte) 6, true),
        UDP((byte) 17, true),
        ICMP_V6((byte) 58, false),
        SCTP((byte) 132, true);

        private final byte protocolCode;
        private final boolean isTransportProtocol;

        Protocol(int ianaNumber, boolean isTransportProtocol) {
            this.protocolCode = Integer.valueOf(ianaNumber).byteValue();
            this.isTransportProtocol = isTransportProtocol;
        }

        public static final Map<Byte, Protocol> protocolCodeMap = Arrays.stream(values())
            .collect(Collectors.toMap(Protocol::getProtocolCode, p -> p));

        public static Protocol fromProtocolName(String protocolName) {
            String name = protocolName.toUpperCase(Locale.ROOT);
            if (name.equals("IPV6-ICMP")) {
                return Protocol.ICMP_V6;
            }
            try {
                return valueOf(name);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        public byte getProtocolCode() {
            return this.protocolCode;
        }

        public boolean isTransportProtocol() {
            return this.isTransportProtocol;
        }
    }

    public static class Factory implements Processor.Factory {
        @Override
        public CommunityIdProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String sourceIPField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "source_ip_field");
            String sourcePortField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "source_port_field");
            String destinationIPField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "destination_ip_field");
            String destinationPortField = ConfigurationUtils.readOptionalStringProperty(
                TYPE,
                processorTag,
                config,
                "destination_port_field"
            );
            String ianaProtocolNumberField = ConfigurationUtils.readOptionalStringProperty(
                TYPE,
                processorTag,
                config,
                "iana_protocol_number_field"
            );
            String protocolField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "protocol_field");
            String icmpTypeField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "icmp_type_field");
            String icmpCodeField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "icmp_code_field");
            int seed = ConfigurationUtils.readIntProperty(TYPE, processorTag, config, "seed", 0);
            if (seed < MIN_SEED || seed > MAX_SEED) {
                throw newConfigurationException(TYPE, processorTag, "seed", "seed must be between 0 and 65535");
            }

            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "community_id");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            return new CommunityIdProcessor(
                processorTag,
                description,
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
}
