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

import java.util.LinkedList;
import java.util.List;
import java.util.Collections;

import org.apache.samza.task.MessageCollector;
import org.apache.samza.system.OutgoingMessageEnvelope;

/**
 * A InMemoryMessageCollector is a Samza MessageCollector that is
 * used in the TaskUnitTestHarness for unit testing Samza Tasks.
 * This in-memory collector allows the possibility of setting
 * and removing a MessageCollectorListener that triggers its
 * onMessageAdded method for every message on which send is called.
 */
public class InMemoryMessageCollector implements MessageCollector {
    private LinkedList<OutgoingMessageEnvelope> inMemoryCollector;
    private LinkedList<MessageCollectorListener> listeners;

    public InMemoryMessageCollector() {
        inMemoryCollector = new LinkedList<OutgoingMessageEnvelope>();
        listeners = new LinkedList<MessageCollectorListener>();
    }

    public void send(OutgoingMessageEnvelope envelope) {
        inMemoryCollector.add(envelope);
        // notify all listeners
        for (int i = 0; i < listeners.size(); i++) {
            MessageCollectorListener listener = listeners.get(i);
            listener.onMessageAdded(envelope);
        }
    }

    public List<MessageCollectorListener> getListeners() {
        return Collections.unmodifiableList(listeners);
    }

    public void registerListener(MessageCollectorListener listener) {
        listeners.add(listener);
    }

    public Boolean hasListeners() {
        return listeners.size() > 0;
    }

    public void removeListener(MessageCollectorListener listener) {
        listeners.remove(listener);
    }

    public List<OutgoingMessageEnvelope> getMessages() {
        return Collections.unmodifiableList(inMemoryCollector);
    }
}