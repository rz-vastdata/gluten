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

package io.glutenproject.utils

import io.glutenproject.GlutenConfig

object SystemParameters {

  val CLICKHOUSE_LIB_PATH_KEY = "clickhouse.lib.path"
  val CLICKHOUSE_LIB_PATH_DEFAULT_VALUE = ""

  val TPCDS_DATA_PATH_KEY = "tpcds.data.path"
  val TPCDS_DATA_PATH_DEFAULT_VALUE = "/data/tpcds-data-sf1"

  def getClickHouseLibPath(): String = {
    System.getProperty(
      SystemParameters.CLICKHOUSE_LIB_PATH_KEY,
      SystemParameters.CLICKHOUSE_LIB_PATH_DEFAULT_VALUE)
  }

  def getTpcdsDataPath(): String = {
    System.getProperty(
      SystemParameters.TPCDS_DATA_PATH_KEY,
      SystemParameters.TPCDS_DATA_PATH_DEFAULT_VALUE)
  }

  def getGlutenBackend(): String = {
    System.getProperty(
      GlutenConfig.GLUTEN_BACKEND_LIB, GlutenConfig.GLUTEN_VELOX_BACKEND)
  }
}