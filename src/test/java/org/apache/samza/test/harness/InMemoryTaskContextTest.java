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

import org.apache.samza.storage.kv.KeyValueStore;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

import java.util.HashMap;
import java.util.Map;

public class InMemoryTaskContextTest {
    private InMemoryTaskContext taskContext;
    private String testString = "test";

    @Before
    public void setup() {
        String classTaskName = "com.uber.athena.driver.TestProcessTask";
        Map<String, String> map = new HashMap<String, String>();
        Config config = new MapConfig(map);
        taskContext = new InMemoryTaskContext<String, String>(classTaskName, config);
    }

    @Test
    public void testStore() {
        KeyValueStore<String, String> store = taskContext.getStore(testString);
        store.put(testString, testString);
        assertEquals(taskContext.getStore(testString).get(testString), store.get(testString));
    }

    @Test
    public void testTaskName() {
        assertEquals(taskContext.getTaskName().getTaskName(), "com.uber.athena.driver.TestProcessTask");
    }

    @Test
    public void testMetricRegistry() {
        MetricsRegistry registry = taskContext.getMetricsRegistry();
        Counter testCounter = registry.newCounter("test", "test");
        testCounter.inc();
        assertEquals(1, testCounter.getCount());
    }
}