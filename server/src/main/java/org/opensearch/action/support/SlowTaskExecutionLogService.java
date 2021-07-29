/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.OpenSearchLogMessage;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.Task;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SlowTaskExecutionLogService {

    private volatile long searchTaskWarnThreshold;
    private volatile long searchTaskInfoThreshold;
    private volatile long searchTaskDebugThreshold;
    private volatile long searchTaskTraceThreshold;

    private TaskSlowLogLevel level;

    static final String searchTaskRegex = "*search*";
    static final String SEARCH_TASK_SLOWLOG_PREFIX = "task.search.slowlog";

    private final Logger searchSlowTaskLogger;

    public static final Setting<TimeValue> SEARCH_TASK_SLOWLOG_THRESHOLD_WARN_SETTING =
        Setting.timeSetting(SEARCH_TASK_SLOWLOG_PREFIX + ".threshold.warn", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<TimeValue> SEARCH_TASK_SLOWLOG_THRESHOLD_INFO_SETTING =
        Setting.timeSetting(SEARCH_TASK_SLOWLOG_PREFIX + ".threshold.info", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<TimeValue> SEARCH_TASK_SLOWLOG_THRESHOLD_DEBUG_SETTING =
        Setting.timeSetting(SEARCH_TASK_SLOWLOG_PREFIX + ".threshold.debug", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<TimeValue> SEARCH_TASK_SLOWLOG_THRESHOLD_TRACE_SETTING =
        Setting.timeSetting(SEARCH_TASK_SLOWLOG_PREFIX + ".threshold.trace", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<TaskSlowLogLevel> SEARCH_TASK_SLOWLOG_LEVEL_SETTING =
        new Setting<>(SEARCH_TASK_SLOWLOG_PREFIX + ".level", TaskSlowLogLevel.TRACE.name(), TaskSlowLogLevel::parse,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    public SlowTaskExecutionLogService(Settings settings, ClusterSettings clusterSettings) {
        this.searchTaskWarnThreshold = SEARCH_TASK_SLOWLOG_THRESHOLD_WARN_SETTING.get(settings).nanos();
        clusterSettings.addSettingsUpdateConsumer(SEARCH_TASK_SLOWLOG_THRESHOLD_WARN_SETTING, this::setSearchTaskWarnThreshold);
        this.searchTaskInfoThreshold = SEARCH_TASK_SLOWLOG_THRESHOLD_INFO_SETTING.get(settings).nanos();
        clusterSettings.addSettingsUpdateConsumer(SEARCH_TASK_SLOWLOG_THRESHOLD_INFO_SETTING, this::setSearchTaskInfoThreshold);
        this.searchTaskDebugThreshold = SEARCH_TASK_SLOWLOG_THRESHOLD_DEBUG_SETTING.get(settings).nanos();
        clusterSettings.addSettingsUpdateConsumer(SEARCH_TASK_SLOWLOG_THRESHOLD_DEBUG_SETTING, this::setSearchTaskDebugThreshold);
        this.searchTaskTraceThreshold = SEARCH_TASK_SLOWLOG_THRESHOLD_TRACE_SETTING.get(settings).nanos();
        clusterSettings.addSettingsUpdateConsumer(SEARCH_TASK_SLOWLOG_THRESHOLD_TRACE_SETTING, this::setSearchTaskTraceThreshold);
        this.level = SEARCH_TASK_SLOWLOG_LEVEL_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SEARCH_TASK_SLOWLOG_LEVEL_SETTING, this::setLevel);
        this.searchSlowTaskLogger = LogManager.getLogger(SEARCH_TASK_SLOWLOG_PREFIX);
        Loggers.setLevel(this.searchSlowTaskLogger, TaskSlowLogLevel.TRACE.name());
    }

    public void maybeLog(Task task, long tookInNanos) {
        if(Regex.simpleMatch(searchTaskRegex, task.getAction())) {
            if (searchTaskWarnThreshold >= 0 && tookInNanos > searchTaskWarnThreshold && level.isLevelEnabledFor(TaskSlowLogLevel.WARN)) {
                searchSlowTaskLogger.warn(new SearchTaskSlowLogMessage(task, tookInNanos));
            } else if (searchTaskInfoThreshold >= 0 && tookInNanos > searchTaskInfoThreshold
                && level.isLevelEnabledFor(TaskSlowLogLevel.INFO)) {
                searchSlowTaskLogger.info(new SearchTaskSlowLogMessage(task, tookInNanos));
            } else if (searchTaskDebugThreshold >= 0 && tookInNanos > searchTaskDebugThreshold
                && level.isLevelEnabledFor(TaskSlowLogLevel.DEBUG)) {
                searchSlowTaskLogger.debug(new SearchTaskSlowLogMessage(task, tookInNanos));
            } else if (searchTaskTraceThreshold >= 0 && tookInNanos > searchTaskTraceThreshold
                && level.isLevelEnabledFor(TaskSlowLogLevel.TRACE)) {
                searchSlowTaskLogger.trace(new SearchTaskSlowLogMessage(task, tookInNanos));
            }
        }
    }

    private void setSearchTaskDebugThreshold(TimeValue debugThreshold) {
        this.searchTaskDebugThreshold = debugThreshold.nanos();
    }

    private void setSearchTaskInfoThreshold(TimeValue debugThreshold) {
        this.searchTaskInfoThreshold = debugThreshold.nanos();
    }

    private void setSearchTaskTraceThreshold(TimeValue debugThreshold) {
        this.searchTaskTraceThreshold = debugThreshold.nanos();
    }

    private void setSearchTaskWarnThreshold(TimeValue debugThreshold) {
        this.searchTaskDebugThreshold = debugThreshold.nanos();
    }

    private void setLevel(TaskSlowLogLevel level) {
        this.level = level;
    }

    public enum TaskSlowLogLevel {
        WARN(3), // most specific - little logging
        INFO(2),
        DEBUG(1),
        TRACE(0); // least specific - lots of logging

        private final int specificity;

        TaskSlowLogLevel(int specificity) {
            this.specificity = specificity;
        }

        public static TaskSlowLogLevel parse(String level) {
            return valueOf(level.toUpperCase(Locale.ROOT));
        }

        boolean isLevelEnabledFor(TaskSlowLogLevel levelToBeUsed) {
            return this.specificity <= levelToBeUsed.specificity;
        }
    }

    static final class SearchTaskSlowLogMessage extends OpenSearchLogMessage {

        SearchTaskSlowLogMessage(Task task, long tookInNanos) {
            super(prepareMap(task, tookInNanos), message(task, tookInNanos));
        }

        private static Map<String, Object> prepareMap(Task task, long tookInNanos) {
            Map<String, Object> messageFields = new HashMap<>();
            messageFields.put("message", task.getId());
            messageFields.put("took", TimeValue.timeValueNanos(tookInNanos));
            messageFields.put("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
            if (task.getParentTaskId() != null && task.getParentTaskId().isSet()) {
                messageFields.put("parent_task_id", task.getParentTaskId());
            }
            if (task.getHeader(Task.X_OPAQUE_ID) != null) {
                messageFields.put("task_header_id",task.getHeader(Task.X_OPAQUE_ID));
            }
            messageFields.put("task_description", task.getDescription());
            messageFields.put("task_type", task.getType());
            messageFields.put("task_status", task.getStatus());
            messageFields.put("task_start_time_millis", task.getStartTime());
            messageFields.put("task_action", task.getAction());
            return messageFields;
        }

        // Message will be used in plaintext logs
        private static String message(Task task, long tookInNanos) {
            StringBuilder sb = new StringBuilder();
            sb.append(task.getId())
                .append(" ")
                .append("parent_task_id[").append(task.getParentTaskId()).append("]")
                .append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], ")
                .append("took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ")
                .append("description[").append(task.getDescription()).append("], ")
                .append("task_type[").append(task.getType()).append("], ")
                .append("task_status[").append(task.getStatus()).append("], ")
                .append("task_start_time_millis[").append(task.getStartTime()).append("], ")
                .append("task_action[").append(task.getAction()).append("], ");

            if (task.getHeader(Task.X_OPAQUE_ID) != null) {
                sb.append("task_header_id[").append(task.getHeader(Task.X_OPAQUE_ID)).append("], ");
            } else {
                sb.append("task_header_id[], ");
            }
            if (task.getParentTaskId() != null && task.getParentTaskId().isSet()) {
                sb.append("parent_task_id[").append(task.getParentTaskId()).append("], ");
            } else {
                sb.append("parent_task_id[], ");
            }
            return sb.toString();
        }
    }
}
