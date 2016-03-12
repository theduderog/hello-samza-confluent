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

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.samza.system.OutgoingMessageEnvelope;

public class TaskUnitTestHarnessTest {
    private String key = "key";
    private Integer value = 10;

    @Test
    public void testStart() throws Exception {
        String classTaskName = "org.apache.samza.test.harness.TestProcessTask";
        TaskUnitTestHarness<String, Integer> testProcessTask = new TaskUnitTestHarness<String, Integer>(classTaskName, false, true);
        testProcessTask.start();
        assertTrue(testProcessTask.isStarted());
    }

    @Test
    public void testRegisterAndRemoveMessageListener() throws Exception {
        String classTaskName = "org.apache.samza.test.harness.TestProcessTask";
        TaskUnitTestHarness<String, String> testProcessTask = new TaskUnitTestHarness<String, String>(classTaskName, false, true);
        MessageCollectorListener listener = new KeyAsserterListener();
        assertEquals(0, testProcessTask.getListeners().size());
        testProcessTask.registerMessageListenerOnProcess(listener);
        assertEquals(1, testProcessTask.getListeners().size());
        testProcessTask.removeMessageListenerOnProcess(listener);
        assertEquals(0, testProcessTask.getListeners().size());
    }

    @Test
    public void testInjectDuringProcess() throws Exception {
        String classTaskName = "org.apache.samza.test.harness.TestProcessTask";
        TaskUnitTestHarness<String, Integer> testProcessTask = new TaskUnitTestHarness<String, Integer>(classTaskName, false, true);
        testProcessTask.start();
        testProcessTask.registerMessageListenerOnProcess(new KeyAsserterListener());
        testProcessTask.inject(key, value);
    }

    @Test
    public void testWindowAndGetResult() throws Exception {
        String classTaskName = "org.apache.samza.test.harness.TestWindowTask";
        TaskUnitTestHarness<String, Integer> testWindowTask = new TaskUnitTestHarness<String, Integer>(classTaskName, true, true, 2);
        testWindowTask.start();
        testWindowTask.registerMessageListenerOnProcess(new KeyAsserterListener());
        assertEquals(0, testWindowTask.getResult().size());
        testWindowTask.inject(key, value);
        assertEquals(0, testWindowTask.getResult().size());
        testWindowTask.inject(key, value);
        assertEquals(2, testWindowTask.getResult().size());
        assertEquals(100, testWindowTask.getResult().get(0).getMessage());
        assertEquals(100, testWindowTask.getResult().get(1).getMessage());
    }

    @Test
    public void testInjectingBeforeStarting() throws Exception {
        try {
            String classTaskName = "org.apache.samza.test.harness.TestProcessTask";
            TaskUnitTestHarness<String, Integer> testProcessTask = new TaskUnitTestHarness<String, Integer>(classTaskName, false, true);
            testProcessTask.inject(key, value);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Trying to inject data into the Driver Unit Testing Interface without first starting it.");
        }
    }

    public class KeyAsserterListener implements MessageCollectorListener {
        public void onMessageAdded(OutgoingMessageEnvelope envelope) {
            assertTrue(envelope.getMessage().equals(10 * 10));
        }
    }
}

