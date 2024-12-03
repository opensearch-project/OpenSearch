/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.pattern;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BrainLogParserTests extends OpenSearchTestCase {

    private static final List<String> TEST_HDFS_LOGS = Arrays.asList(
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to blk_-7017553867379051457 size 67108864",
        "BLOCK* NameSystem.allocateBlock: /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296. blk_-6620182933895093708",
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.7.244:50010 is added to blk_-6956067134432991406 size 67108864",
        "BLOCK* NameSystem.allocateBlock: /user/root/sortrand/_temporary/_task_200811092030_0002_r_000230_0/part-00230. blk_559204981722276126",
        "BLOCK* NameSystem.allocateBlock: /user/root/sortrand/_temporary/_task_200811092030_0002_r_000169_0/part-00169. blk_-7105305952901940477",
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added to blk_-3249711809227781266 size 67108864",
        "BLOCK* NameSystem.allocateBlock: /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318. blk_-207775976836691685",
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.6.4:50010 is added to blk_5114010683183383297 size 67108864",
        "BLOCK* NameSystem.allocateBlock: /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318. blk_2096692261399680562",
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.15.240:50010 is added to blk_-1055254430948037872 size 67108864",
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.250.7.146:50010 is added to blk_278357163850888 size 67108864",
        "BLOCK* NameSystem.allocateBlock: /user/root/sortrand/_temporary/_task_200811092030_0002_r_000138_0/part-00138. blk_-210021574616486609",
        "Verification succeeded for blk_-1547954353065580372",
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.39.242:50010 is added to blk_-4110733372292809607 size 67108864",
        "BLOCK* NameSystem.allocateBlock: /user/root/randtxt/_temporary/_task_200811092030_0003_m_000382_0/part-00382. blk_8935202950442998446",
        "BLOCK* NameSystem.allocateBlock: /user/root/randtxt/_temporary/_task_200811092030_0003_m_000392_0/part-00392. blk_-3010126661650043258",
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.25.237:50010 is added to blk_541463031152673662 size 67108864",
        "Verification succeeded for blk_6996194389878584395",
        "PacketResponder failed for blk_6996194389878584395",
        "PacketResponder failed for blk_-1547954353065580372"
    );

    private BrainLogParser parser;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        parser = new BrainLogParser();
    }

    public void testNewParserWithIllegalArgument() {
        String exceptionMessage = "Threshold percentage must be between 0.0 and 1.0";
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> new BrainLogParser(2, -1.0f));
        assertEquals(exceptionMessage, throwable.getMessage());
        throwable = assertThrows(IllegalArgumentException.class, () -> new BrainLogParser(2, 1.1f));
        assertEquals(exceptionMessage, throwable.getMessage());
    }

    public void testPreprocess() {
        String logMessage = "127.0.0.1 - 1234 something";
        String logId = "log1";
        List<String> expectedResult = Arrays.asList("<*>", "", "<*>", "something", "log1");
        List<String> result = parser.preprocess(logMessage, logId);
        assertEquals(expectedResult, result);

        // Test with different delimiter
        logMessage = "127.0.0.1=1234 something";
        logId = "log2";
        expectedResult = Arrays.asList("<*><*>", "something", "log2");
        result = parser.preprocess(logMessage, logId);
        assertEquals(expectedResult, result);
    }

    public void testPreprocessWithIllegalInput() {
        String logMessage = "127.0.0.1 - 1234 something";
        String logId = "log1";
        String exceptionMessage = "log message or logId must not be null";
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> parser.preprocess(null, logId));
        assertEquals(exceptionMessage, throwable.getMessage());
        throwable = assertThrows(IllegalArgumentException.class, () -> parser.preprocess(logMessage, null));
        assertEquals(exceptionMessage, throwable.getMessage());
        throwable = assertThrows(IllegalArgumentException.class, () -> parser.preprocess(null, null));
        assertEquals(exceptionMessage, throwable.getMessage());
    }

    public void testPreprocessAllLogs() {
        List<String> logMessages = Arrays.asList("127.0.0.1 - 1234 something", "192.168.0.1 - 5678 something_else");
        List<String> logIds = Arrays.asList("log1", "log2");

        List<List<String>> result = parser.preprocessAllLogs(logMessages, logIds);

        assertEquals(2, result.size());
        assertEquals(Arrays.asList("<*>", "", "<*>", "something", "log1"), result.get(0));
        assertEquals(Arrays.asList("<*>", "", "<*>", "something_else", "log2"), result.get(1));
    }

    public void testProcessTokenHistogram() {
        String something = String.format(Locale.ROOT, "%d-%s", 0, "something");
        String up = String.format(Locale.ROOT, "%d-%s", 1, "up");
        List<String> firstTokens = Arrays.asList("something", "up", "0");
        parser.processTokenHistogram(firstTokens);
        assertEquals(1L, parser.getTokenFreqMap().get(something).longValue());
        assertEquals(1L, parser.getTokenFreqMap().get(up).longValue());

        List<String> secondTokens = Arrays.asList("something", "down", "1");
        parser.processTokenHistogram(secondTokens);
        assertEquals(2L, parser.getTokenFreqMap().get(something).longValue());
        assertEquals(1L, parser.getTokenFreqMap().get(up).longValue());
    }

    public void testCalculateGroupTokenFreq() {
        List<String> logMessages = Arrays.asList(
            "127.0.0.1 - 1234 something",
            "192.168.0.1:5678 something_else",
            "0.0.0.0:42 something_else"
        );
        List<String> logIds = Arrays.asList("log1", "log2", "log3");

        List<List<String>> preprocessedLogs = parser.preprocessAllLogs(logMessages, logIds);
        parser.calculateGroupTokenFreq(preprocessedLogs);

        for (String logId : logIds) {
            String groupCandidate = parser.getLogIdGroupCandidateMap().get(logId);
            assertNotNull(groupCandidate);
        }
        assertTrue(parser.getGroupTokenSetMap().containsValue(Set.of("something")));
        assertTrue(parser.getGroupTokenSetMap().containsValue(Set.of("something_else")));
        String sampleGroupTokenKey = String.format(Locale.ROOT, "%d-%s-%d", 4, parser.getLogIdGroupCandidateMap().get("log1"), 3);
        assertTrue(parser.getGroupTokenSetMap().get(sampleGroupTokenKey).contains("something"));
    }

    public void testCalculateGroupTokenFreqWithIllegalInput() {
        List<List<String>> preprocessedLogs = Arrays.asList(List.of());
        String exceptionMessage = "Sorted word combinations must be non empty";
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> parser.calculateGroupTokenFreq(preprocessedLogs));
        assertEquals(exceptionMessage, throwable.getMessage());
    }

    public void testParseLogPattern() {
        List<List<String>> preprocessedLogs = parser.preprocessAllLogs(TEST_HDFS_LOGS, List.of());
        parser.calculateGroupTokenFreq(preprocessedLogs);

        List<String> expectedLogPattern = Arrays.asList(
            "BLOCK*",
            "NameSystem.addStoredBlock",
            "blockMap",
            "updated",
            "<*>",
            "is",
            "added",
            "to",
            "blk_<*>",
            "size",
            "<*>"
        );
        List<String> logPattern = parser.parseLogPattern(preprocessedLogs.get(0));
        assertEquals(expectedLogPattern, logPattern);
    }

    public void testParseAllLogPatterns() {
        Map<String, List<String>> logPatternMap = parser.parseAllLogPatterns(TEST_HDFS_LOGS, List.of());

        Map<String, Integer> expectedResult = Map.of(
            "PacketResponder failed for blk_<*>",
            2,
            "Verification succeeded for blk_<*>",
            2,
            "BLOCK* NameSystem.addStoredBlock blockMap updated <*> is added to blk_<*> size <*>",
            8,
            "BLOCK* NameSystem.allocateBlock <*> blk_<*>",
            8
        );
        Map<String, Integer> logPatternByCountMap = logPatternMap.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
        assertEquals(expectedResult, logPatternByCountMap);
    }

    public void testParseLogPatternWhenLowerFrequencyTokenIsVariable() {
        int testVariableCountThreshold = 3;
        parser = new BrainLogParser(testVariableCountThreshold);
        List<String> logMessages = Arrays.asList(
            "Verification succeeded a blk_-1547954353065580372",
            "Verification succeeded b blk_6996194389878584395",
            "Verification succeeded c blk_6996194389878584395",
            "Verification succeeded d blk_6996194389878584395"
        );

        Map<String, List<String>> expectedResult = Map.of("Verification succeeded <*> blk_<*>", Arrays.asList("0", "1", "2", "3"));
        Map<String, List<String>> logPatternMap = parser.parseAllLogPatterns(logMessages, List.of());
        assertEquals(expectedResult, logPatternMap);
        /*
         * 'a', 'b', 'c' and 'd' token is on the 3rd position in the group 2,3, their frequency is lower than
         * representative frequency. Since that position's distinct token number exceeds the variable count threshold,
         * the third position in this log group is treated as variable
         */
        assertTrue(parser.getTokenFreqMap().get("2-a") < parser.getTokenFreqMap().get("0-Verification"));
        assertTrue(parser.getTokenFreqMap().get("2-b") < parser.getTokenFreqMap().get("0-Verification"));
        assertTrue(testVariableCountThreshold <= parser.getGroupTokenSetMap().get("4-4,3-2").size());
    }

    public void testParseLogPatternWhenHigherFrequencyTokenIsVariable() {
        List<String> logMessages = Arrays.asList(
            "Verification succeeded for blk_-1547954353065580372",
            "Verification succeeded for blk_6996194389878584395",
            "Test succeeded for blk_6996194389878584395",
            "Verification",
            "Verification"
        );

        Map<String, List<String>> expectedResult = Map.of(
            "<*> succeeded for blk_<*>",
            Arrays.asList("0", "1", "2"),
            "Verification",
            Arrays.asList("3", "4")
        );
        Map<String, List<String>> logPatternMap = parser.parseAllLogPatterns(logMessages, List.of());
        assertEquals(expectedResult, logPatternMap);
        /*
         * 'Verification' and 'Test' token is on the 1st position in the group 3,3, 'Verification' frequency is higher than
         * representative frequency because there are other groups which have 'Verification' token on the 1st position as well.
         * Since first position's distinct token number is not unique, 'Verification' is treated as variable eventually.
         */
        assertTrue(parser.getTokenFreqMap().get("0-Verification") > parser.getTokenFreqMap().get("1-succeeded"));
        assertTrue(parser.getGroupTokenSetMap().get("4-3,3-0").size() > 1);
    }
}
