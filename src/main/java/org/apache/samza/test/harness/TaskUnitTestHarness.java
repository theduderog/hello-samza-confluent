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

import org.apache.samza.storage.kv.KeyValueStorageEngine;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.ReadableCoordinator;

import java.lang.reflect.Method;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * A TaskUnitTestHarness is an interface to cleanly and succinctly
 * unit test Samza Tasks. Once can manually inject several <key,value>
 * pairs that would normally be passed in by the `SystemStream` provided
 * in `tasks.input`. The same interface lets you visualize all the
 * corresponding <key,value> pairs sent to a custom `Collector`, which are
 * the output of the `process`ed or `window`ed input <key,value> pairs.
 *
 * @param <K> Instance of the key to be injected in the harness for testing.
 * @param <V> Instance of the value to be injected in the harness for testing.
 */
public class TaskUnitTestHarness<K, V> {
    private final Boolean isWindowable;
    private final Boolean isInitiable;
    private static final int DEFAULT_WINDOW_PER_PROCESS = 3;
    private final int windowPerProcess;
    private final String classTaskName;
    private final Object task;
    private final Class<?> taskClass;
    private final InMemoryMessageCollector messageCollector;
    private TaskCoordinator taskCoordinator;
    private InMemoryTaskContext taskContext;
    private boolean isStarted;
    private int timesProcessed;
    private Method init;
    private Method process;
    private Method window;
    private Object[] argProcess;
    private Object[] argWindow;
    private Object[] argInit;

    /**
     * Constructor
     *
     * @param classTaskName    (required) complete class name of the task to be unit tested using the harness.
     * @param isWindowable     (required) boolean that describes whether this is a WindowableTask
     * @param isInitiable      (required) boolean that describes whether this is an InitiableTask
     * @param windowPerProcess (optional) the number of times to trigger Window wrt the number of processed
     *                         or injected messages. In other words if `windowPerProcess = 3`, for every three messages injected, the
     *                         task's window method will be triggered once
     */
    public TaskUnitTestHarness(String classTaskName, Boolean isWindowable, Boolean isInitiable, int windowPerProcess) throws Exception {
        this.isWindowable = isWindowable;
        this.isInitiable = isInitiable;
        this.windowPerProcess = windowPerProcess;
        this.classTaskName = classTaskName;

        //setup
        isStarted = false;
        timesProcessed = 0;
        messageCollector = new InMemoryMessageCollector();
        taskClass = Class.forName(classTaskName);

        if (isInitiable) {
            Class[] initTypes = new Class[2];
            initTypes[0] = Config.class;
            initTypes[1] = TaskContext.class;
            init = taskClass.getMethod("init", initTypes);
        }

        if (isWindowable) {
            Class[] windowTypes = new Class[2];
            windowTypes[0] = MessageCollector.class;
            windowTypes[1] = TaskCoordinator.class;
            window = taskClass.getMethod("window", windowTypes);
        }

        //process
        Class[] processTypes = new Class[3];
        processTypes[0] = IncomingMessageEnvelope.class;
        processTypes[1] = MessageCollector.class;
        processTypes[2] = TaskCoordinator.class;
        process = taskClass.getMethod("process", processTypes);

        //dynamically create object using reflections
        task = taskClass.newInstance();

        // initialize method argument arrays
        argProcess = new Object[3];
        argWindow = new Object[2];
        argInit = new Object[2];
    }

    public TaskUnitTestHarness(String classTaskName, Boolean isWindowable, Boolean isInitiable) throws Exception {
        this(classTaskName, isWindowable, isInitiable, DEFAULT_WINDOW_PER_PROCESS);
    }

    public synchronized void start() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        Config config = new MapConfig(map);
        this.start(config);
    }

    public synchronized void start(Config config) throws Exception {
        taskContext = new InMemoryTaskContext(classTaskName, config);
        taskCoordinator = new ReadableCoordinator(taskContext.getTaskName());
        if (isInitiable) {
            argInit[0] = config;
            argInit[1] = taskContext;
            init.invoke(task, argInit);
        }
        isStarted = true;
    }

    public synchronized void inject(K key, V value) throws Exception {
        if (isStarted) {
            argProcess[0] = new SimpleIncomingMessageEnvelope<K, V>(key, value);
            argProcess[1] = messageCollector;
            argProcess[2] = taskCoordinator;
            process.invoke(task, argProcess);
            timesProcessed++;

            if (isWindowable && timesProcessed == windowPerProcess) {
                argWindow[0] = messageCollector;
                argWindow[1] = taskCoordinator;
                window.invoke(task, argWindow);
                timesProcessed = 0;
            }
        } else {
            throw new Exception("Trying to inject data into the Driver Unit Testing Interface without first starting it.");
        }
    }

    public void registerMessageListenerOnProcess(MessageCollectorListener listener) {
        messageCollector.registerListener(listener);
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void removeMessageListenerOnProcess(MessageCollectorListener listener) {
        messageCollector.removeListener(listener);
    }

    public List<MessageCollectorListener> getListeners() {
        return messageCollector.getListeners();
    }

    public List<OutgoingMessageEnvelope> getResult() {
        return messageCollector.getMessages();
    }

    public KeyValueStore getKVStore() {
        return taskContext.getStore("");
    }
}