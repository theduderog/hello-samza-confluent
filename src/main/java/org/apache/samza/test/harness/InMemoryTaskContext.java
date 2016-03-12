/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.test.harness;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;
import org.apache.samza.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

/**
 * An InMemoryTaskContext<K,V> is a Samza TaskContext that is
 * used in the TaskUnitTestHarness for unit testing Samza Tasks.
 * The InMemoryTaskContext maintains all necessary the constructs
 * for TaskContext in memory.
 *
 * @param <K> Instance of the key to be injected in the in-memory store.
 * @param <V> Instance of the value to be injected in the in-memory store.
 */
public class InMemoryTaskContext<K, V> implements TaskContext {
    private MetricsRegistry registry;
    private Set<SystemStreamPartition> systemStreamPartitions;
    private Map store;
    private TaskName taskName;
    private SamzaContainerContext containerContext;
    private static final Logger log = LoggerFactory.getLogger(InMemoryTaskContext.class);

    public InMemoryTaskContext(String classTaskName, Config config) {
        store = new HashMap<K, V>();
        registry = new MetricsRegistryMap();
        systemStreamPartitions = new HashSet<SystemStreamPartition>();
        taskName = new TaskName(classTaskName);
        ArrayList<TaskName> taskNames = new ArrayList<TaskName>();
        taskNames.add(taskName);
        containerContext = new SamzaContainerContext(0, config, taskNames);
    }

    public MetricsRegistry getMetricsRegistry() {
        return registry;
    }

    public Set<SystemStreamPartition> getSystemStreamPartitions() {
        return systemStreamPartitions;
    }

    public Map<K, V> getStore(String name) {
        return store;
    }

    public TaskName getTaskName() {
        return taskName;
    }

    public void setStartingOffset(SystemStreamPartition ssp, String offset) {
        log.error("DriverTaskContext does not implement this.");
    }

    public SamzaContainerContext getSamzaContainerContext() {
        return containerContext;
    }
}