/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.fetcher

import ai.chronon.api._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.SparkTestBase
import ai.chronon.spark.{Join => _, _}
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone

class FetcherUniqueTopKTest extends SparkTestBase {


  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  it should "test temporal fetch join deterministic with UniqueTopK struct" in {
    val namespace = "deterministic_unique_topk_fetch"
    val joinConf = FetcherTestUtil.generateMutationDataWithUniqueTopK(namespace, tableUtils, spark)
    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         "2021-04-10",
                                         namespace,
                                         consistencyCheck = false,
                                         dropDsOnWrite = true)(spark)
  }

  it should "test temporal fetch join with UniqueTopK UPDATE mode" in {
    val namespace = "unique_topk_last_seen_fetch"
    SparkTestBase.createDatabase(spark, namespace)

    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

    // Event data for queries
    val eventData = Seq(
      Row(1L, toTs("2021-04-10 09:00:00"), "2021-04-10")
    )

    // Struct snapshot data with duplicates to test UPDATE
    val structData = Seq(
      Row(1L, toTs("2021-04-04 00:30:00"), Row("z", 1L, 100), "2021-04-09"),  // ID=1, first occurrence
      Row(1L, toTs("2021-04-05 00:30:00"), Row("y", 2L, 200), "2021-04-09"),  // ID=2
      Row(1L, toTs("2021-04-06 00:30:00"), Row("x", 3L, 300), "2021-04-09"),  // ID=3
      Row(1L, toTs("2021-04-07 00:30:00"), Row("w", 1L, 400), "2021-04-09")   // ID=1 again - should REPLACE
    )

    // Mutation data with another replacement for ID=1
    val mutationData = Seq(
      Row(1L, toTs("2021-04-08 00:30:00"), Row("v", 1L, 500), "2021-04-09", toTs("2021-04-08 00:30:00"), false)  // ID=1 UPDATE
    )

    // Schemas
    val eventSchema = StructType(
      "listing_events_last_seen",
      Array(StructField("listing_id", LongType), StructField("ts", LongType), StructField("ds", StringType))
    )

    val structSnapshotSchema = StructType(
      "listing_struct_snapshot_last_seen",
      Array(
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("rating_struct",
                    StructType("RatingStruct",
                               Array(
                                 StructField("sort_key", StringType),
                                 StructField("unique_id", LongType),
                                 StructField("value", IntType)
                               ))),
        StructField("ds", StringType)
      )
    )

    val structMutationSchema = StructType(
      "listing_struct_mutation_last_seen",
      Array(
        StructField("listing_id", LongType),
        StructField("ts", LongType),
        StructField("rating_struct",
                    StructType("RatingStruct",
                               Array(
                                 StructField("sort_key", StringType),
                                 StructField("unique_id", LongType),
                                 StructField("value", IntType)
                               ))),
        StructField("ds", StringType),
        StructField("mutation_time", LongType),
        StructField("is_before_reversal", BooleanType)
      )
    )

    spark
      .createDataFrame(eventData.toJava, SparkConversions.fromChrononSchema(eventSchema))
      .save(s"$namespace.${eventSchema.name}")

    spark
      .createDataFrame(structData.toJava, SparkConversions.fromChrononSchema(structSnapshotSchema))
      .save(s"$namespace.${structSnapshotSchema.name}")

    spark
      .createDataFrame(mutationData.toJava, SparkConversions.fromChrononSchema(structMutationSchema))
      .save(s"$namespace.${structMutationSchema.name}")

    val structSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Map("listing_id" -> "listing_id", "ts" -> "ts", "rating_struct" -> "rating_struct"),
        startPartition = "2021-04-01",
        endPartition = "2021-04-10",
        mutationTimeColumn = "mutation_time",
        reversalColumn = "is_before_reversal"
      ),
      snapshotTable = s"$namespace.${structSnapshotSchema.name}",
      mutationTable = s"$namespace.${structMutationSchema.name}",
      mutationTopic = "blank"
    )

    val leftSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("listing_id", "ts"),
        startPartition = "2021-04-01"
      ),
      table = s"$namespace.${eventSchema.name}"
    )

    val groupBy = Builders.GroupBy(
      sources = Seq(structSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.UNIQUE_TOP_K,
          inputColumn = "rating_struct",
          argMap = Map("k" -> "3", "collision_strategy" -> "UPDATE"),  // UPDATE mode
          windows = null
        )
      ),
      derivations = Seq(
        Builders.Derivation("ids", "transform(rating_struct_unique_top3, x -> x.unique_id)"),
        Builders.Derivation("values", "transform(rating_struct_unique_top3, x -> x.value)")
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test.struct_unique_topk_last_seen_gb", namespace = namespace, team = "chronon")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test.struct_unique_topk_last_seen_join", namespace = namespace, team = "chronon")
    )

    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         "2021-04-10",
                                         namespace,
                                         consistencyCheck = false,
                                         dropDsOnWrite = true)(spark)
  }
}
