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

package io.glutenproject.execution

import org.apache.spark.SparkConf
import java.nio.file.Files
import java.io.PrintWriter

class VastSuite extends WholeStageTransformerSuite {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    .set("spark.driver.bindAddress", "127.0.0.1")
    .set("spark.driver.host", "127.0.0.1")
    .set("spark.driver.port", "8888")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
  //    .set("spark.sql.planChangeLog.level", "INFO")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("INFO")

    createTPCHNotNullTables()
  }

  test("simple data frame") {
    val filePath = Files.createTempFile("dummy", ".vast")
    val pw = new PrintWriter(filePath.toFile)
    pw.write(" ")
    pw.close()

    logInfo(f"created a dummy file at $filePath")
    spark.read
      .format("vast")
      .schema("a INT, b STRING, c DOUBLE")
      .load(filePath.toString)
      .createOrReplaceTempView("foobar")
    val df = spark.sql("SELECT b FROM foobar WHERE a = 3")
    df.explain()

    val result = df.collect()
    assert(result.length == 0)
  }
}
