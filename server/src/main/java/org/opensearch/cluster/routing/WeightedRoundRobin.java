/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import java.util.ArrayList;
import java.util.List;

/**
 * Weighted Round Robin Scheduling policy
 *
 */
public class WeightedRoundRobin<T> {

    private List<WeightedRoundRobin.Entity<T>> entities;

    public WeightedRoundRobin(List<WeightedRoundRobin.Entity<T>> entities) {
        this.entities = entities;
    }

    /**
     * *
     * @return list of entities that is ordered using weighted round-robin scheduling
     * http://kb.linuxvirtualserver.org/wiki/Weighted_Round-Robin_Scheduling
     */
    public List<WeightedRoundRobin.Entity<T>> orderEntities() {
        int lastSelectedEntity = -1;
        int size = entities.size();
        double currentWeight = 0;
        List<WeightedRoundRobin.Entity<T>> orderedWeight = new ArrayList<>();
        if (size == 0) {
            return null;
        }
        // Find maximum weight and greatest common divisor of weight across all entities
        double maxWeight = 0;
        double sumWeight = 0;
        Double gcd = null;
        for (WeightedRoundRobin.Entity<T> entity : entities) {
            maxWeight = Math.max(maxWeight, entity.getWeight());
            gcd = (gcd == null) ? entity.getWeight() : gcd(gcd, entity.getWeight());
            sumWeight += entity.getWeight() > 0 ? entity.getWeight() : 0;
        }
        int count = 0;
        while (count < sumWeight) {
            lastSelectedEntity = (lastSelectedEntity + 1) % size;
            if (lastSelectedEntity == 0) {
                currentWeight = currentWeight - gcd;
                if (currentWeight <= 0) {
                    currentWeight = maxWeight;
                    if (currentWeight == 0) {
                        return orderedWeight;
                    }
                }
            }
            if (entities.get(lastSelectedEntity).getWeight() >= currentWeight) {
                orderedWeight.add(entities.get(lastSelectedEntity));
                count++;
            }
        }
        return orderedWeight;
    }

    /**
     * Return greatest common divisor for two integers
     * https://en.wikipedia.org/wiki/Greatest_common_divisor#Using_Euclid.27s_algorithm
     *
     * @param a first number
     * @param b second number
     * @return greatest common divisor
     */
    private double gcd(double a, double b) {
        return (b == 0) ? a : gcd(b, a % b);
    }

    static final class Entity<T> {
        private double weight;
        private T target;

        public Entity(double weight, T target) {
            this.weight = weight;
            this.target = target;
        }

        public T getTarget() {
            return this.target;
        }

        public void setTarget(T target) {
            this.target = target;
        }

        public double getWeight() {
            return this.weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }
    }

}
