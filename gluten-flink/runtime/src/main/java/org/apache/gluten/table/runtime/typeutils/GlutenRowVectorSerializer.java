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
package org.apache.gluten.table.runtime.typeutils;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.Closeable;
import java.io.IOException;

public class GlutenRowVectorSerializer extends TypeSerializer<RowVector> implements Closeable {
  private transient MemoryManager memoryManager;
  private transient Session session;
  private RowType rowType;

  public GlutenRowVectorSerializer(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public TypeSerializer<RowVector> duplicate() {
    return new GlutenRowVectorSerializer(rowType);
  }

  // For a operator with mulitple outputs, Flink will pass the same RowVectorRef to different
  // outputs,
  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public RowVector createInstance() {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void serialize(RowVector record, DataOutputView target) throws IOException {
    String vectorStr = record.serialize();
    target.writeInt(vectorStr.getBytes().length);
    target.write(vectorStr.getBytes());
  }

  @Override
  public RowVector deserialize(DataInputView source) throws IOException {
    initializeSessionIfNeeded();

    int len = source.readInt();
    byte[] str = new byte[len];
    source.readFully(str);
    RowVector rowVector = session.baseVectorOps().deserializeOne(new String(str)).asRowVector();
    return rowVector;
  }

  @Override
  public RowVector deserialize(RowVector reuse, DataInputView source) throws IOException {
    throw new RuntimeException("Not implemented for gluten");
  }

  private void initializeSessionIfNeeded() {
    if (memoryManager == null) {
      memoryManager = MemoryManager.create(AllocationListener.NOOP);
      session = Velox4j.newSession(memoryManager);
    }
  }

  @Override
  public void close() {
    if (memoryManager != null) {
      memoryManager.close();
      session.close();
    }
  }

  @Override
  public TypeSerializerSnapshot<RowVector> snapshotConfiguration() {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public int hashCode() {
    if (rowType == null) {
      return 0;
    }
    return rowType.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GlutenRowVectorSerializer) {
      if (rowType != null) {
        GlutenRowVectorSerializer other = (GlutenRowVectorSerializer) obj;
        return rowType.equals(other.rowType);
      }
      return true;
    }

    return false;
  }

  @Override
  public RowVector copy(RowVector from, RowVector reuse) {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public RowVector copy(RowVector from) {
    // throw new RuntimeException("Not implemented for gluten");
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }
}
