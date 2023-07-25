/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.paimon.spark

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.options.Options
import org.apache.paimon.spark.commands.WriteIntoPaimonTable
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}

import org.apache.spark.sql.{DataFrame, SaveMode => SparkSaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._

class SparkSource
  extends DataSourceRegister
  with SessionConfigSupport
  with CreatableRelationProvider {

  override def shortName(): String = SparkSource.NAME

  override def keyPrefix(): String = SparkSource.NAME

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // ignore schema.
    // getTable will get schema by itself.
    null
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    // ignore partition.
    // getTable will get partition by itself.
    null
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new SparkTable(loadTable(properties))
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SparkSaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val table = loadTable(parameters.asJava)
    WriteIntoPaimonTable(table, SaveMode.transform(mode), data).run(sqlContext.sparkSession)
    SparkSource.toBaseRelation(table, sqlContext)
  }

  private def loadTable(options: util.Map[String, String]): FileStoreTable = {
    val catalogContext = CatalogContext.create(
      Options.fromMap(options),
      SparkSession.active.sessionState.newHadoopConf())
    FileStoreTableFactory.create(catalogContext)
  }
}

object SparkSource {

  val NAME = "paimon"

  def toBaseRelation(table: FileStoreTable, _sqlContext: SQLContext): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = _sqlContext
      override def schema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())
    }
  }
}