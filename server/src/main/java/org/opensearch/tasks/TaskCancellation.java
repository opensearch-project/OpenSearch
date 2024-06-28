/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.ExceptionsHelper;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TaskCancellation represents a task eligible for cancellation.
 * It doesn't guarantee that the task will actually get cancelled or not; that decision is left to the caller.
 * <p>
 * It contains a list of cancellation reasons along with callbacks that are invoked when cancel() is called.
 *
 * @opensearch.internal
 */
public class TaskCancellation implements Comparable<TaskCancellation> {
    private final CancellableTask task;
    private final List<Reason> reasons;
    private final List<Runnable> onCancelCallbacks;

    public TaskCancellation(CancellableTask task, List<Reason> reasons, List<Runnable> onCancelCallbacks) {
        this.task = task;
        this.reasons = reasons;
        this.onCancelCallbacks = onCancelCallbacks;
    }

    public CancellableTask getTask() {
        return task;
    }

    public List<Reason> getReasons() {
        return reasons;
    }

    public List<Runnable> getOnCancelCallbacks() {
        return onCancelCallbacks;
    }

    public String getReasonString() {
        return reasons.stream().map(Reason::getMessage).collect(Collectors.joining(", "));
    }

    public TaskCancellation merge(final TaskCancellation other) {
        if (other == this) {
            return this;
        }
        final List<Reason> newReasons = new ArrayList<>(reasons);
        newReasons.addAll(other.getReasons());
        final List<Runnable> newOnCancelCallbacks = new ArrayList<>(onCancelCallbacks);
        newOnCancelCallbacks.addAll(other.onCancelCallbacks);
        return new TaskCancellation(task, newReasons, newOnCancelCallbacks);
    }

    /**
     * Cancels the task and invokes all onCancelCallbacks.
     */
    public void cancel() {
        if (isEligibleForCancellation() == false) {
            return;
        }

        task.cancel(getReasonString());
        runOnCancelCallbacks();
    }

    /**
     *  Cancels the task and its descendants and invokes all onCancelCallbacks.
     */
    public void cancelTaskAndDescendants(TaskManager taskManager) {
        if (isEligibleForCancellation() == false) {
            return;
        }

        taskManager.cancelTaskAndDescendants(task, getReasonString(), false, ActionListener.wrap(() -> {}));
        runOnCancelCallbacks();
    }

    /**
     * invokes all onCancelCallbacks.
     */
    private void runOnCancelCallbacks() {
        List<Exception> exceptions = new ArrayList<>();
        for (Runnable callback : onCancelCallbacks) {
            try {
                callback.run();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    /**
     * Returns the sum of all cancellation scores.
     * <p>
     * A zero score indicates no reason to cancel the task.
     * A task with a higher score suggests greater possibility of recovering the node when it is cancelled.
     */
    public int totalCancellationScore() {
        return reasons.stream().mapToInt(Reason::getCancellationScore).sum();
    }

    /**
     * A task is eligible for cancellation if it has one or more cancellation reasons, and is not already cancelled.
     */
    public boolean isEligibleForCancellation() {
        return (task.isCancelled() == false) && (reasons.size() > 0);
    }

    @Override
    public int compareTo(TaskCancellation other) {
        return Integer.compare(totalCancellationScore(), other.totalCancellationScore());
    }

    /**
     * Represents the cancellation reason for a task.
     */
    public static class Reason {
        private final String message;
        private final int cancellationScore;

        public Reason(String message, int cancellationScore) {
            this.message = message;
            this.cancellationScore = cancellationScore;
        }

        public String getMessage() {
            return message;
        }

        public int getCancellationScore() {
            return cancellationScore;
        }
    }
}
