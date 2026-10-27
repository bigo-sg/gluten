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
package org.apache.gluten.component

import org.apache.gluten.substrait.rel.PaimonLocalFilesBuilder

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class CHPaimonComponentSuite extends AnyFunSuite {
  test("Paimon component declares the ClickHouse backend dependency") {
    val loader = Thread.currentThread().getContextClassLoader
    val component = "org.apache.gluten.component.CHPaimonComponent"
    assume(loader.getResource(s"META-INF/gluten-components/$component") != null)
    assert(loader.loadClass(component).getDeclaredConstructor().newInstance()
      .asInstanceOf[Component].dependencies().contains(
        classOf[org.apache.gluten.backendsapi.clickhouse.CHBackend]))
  }

  test("Paimon C++ split carries branch") {
    val node = PaimonLocalFilesBuilder.makeNativePaimonLocalFiles(
      0,
      "file:///tmp/paimon-table",
      "review_branch",
      Seq(Array[Byte](1)).asJava,
      Seq.empty[String].asJava)
    val cppSplit = node.toProtobuf.getItems(0).getPaimon.getCppSplit
    assert(cppSplit.getBranch == "review_branch")
    assert(cppSplit.getSerializedDataSplit.toByteArray.toSeq == Seq[Byte](1))
  }
}
