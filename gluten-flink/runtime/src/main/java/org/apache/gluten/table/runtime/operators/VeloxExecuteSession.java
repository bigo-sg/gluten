/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.table.runtime.operators;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.session.Session;

public class VeloxExecuteSession {
    private Session session;
    private MemoryManager memoryManager;
    private BufferAllocator allocator;

    public VeloxExecuteSession() {
        memoryManager = MemoryManager.create(AllocationListener.NOOP);
        session = Velox4j.newSession(memoryManager);
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    public Session getSession() {
        return session;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public void close() throws Exception {
        session.close();
        memoryManager.close();
    }

    UpIterator execute(Query query) {
        return session.queryOps().execute(query);
    }

    public void finish() {}
}
