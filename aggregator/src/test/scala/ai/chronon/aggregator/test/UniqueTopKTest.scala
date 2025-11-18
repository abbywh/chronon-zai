package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.UniqueTopKAggregator
import ai.chronon.api._
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import scala.collection.JavaConverters._
import scala.util.Random

class UniqueTopKAggregatorTest extends AnyFlatSpec {

  "UniqueTopKAggregator with IntType" should "return top k unique integers" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(5, 3, 8, 3, 1, 9, 5, 2)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List(9, 8, 5), resultList)
  }

  "UniqueTopKAggregator with IntType" should "handle duplicates correctly" in {
    val k = 5
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(1, 1, 1, 2, 2, 3, 3, 3, 3)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(3, result.size()) // Only 3 unique values
    assertEquals(List(3, 2, 1), resultList)
  }

  "UniqueTopKAggregator with LongType" should "return top k unique longs" in {
    val k = 2
    val aggregator = new UniqueTopKAggregator[Long](LongType, k)

    val inputs = List(100L, 200L, 50L, 300L, 100L)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List(300L, 200L), resultList)
  }

  "UniqueTopKAggregator with StringType" should "return top k unique strings" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[String](StringType, k)

    val inputs = List("apple", "banana", "cherry", "apple", "date", "banana")
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List("date", "cherry", "banana"), resultList)
  }

  "UniqueTopKAggregator with StructType" should "return top k unique structs" in {
    val structType = StructType("TestStruct",
                                Array(
                                  StructField("sort_key", StringType),
                                  StructField("unique_id", LongType),
                                  StructField("value", IntType)
                                ))

    val k = 2
    val aggregator = new UniqueTopKAggregator[Array[Any]](structType, k)

    val inputs = List(
      Array("z", 1L, 10),
      Array("y", 2L, 20),
      Array("x", 3L, 30),
      Array("z", 1L, 40), // Duplicate unique_id
      Array("w", 4L, 50)
    )

    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals("z", resultList(0)(0)) // Top by sort_key
    assertEquals("y", resultList(1)(0)) // Second by sort_key
  }

  "UniqueTopKAggregator with StructType" should "validate struct field requirements" in {
    val invalidStructType = StructType("InvalidStruct",
                                       Array(
                                         StructField("wrong_field", StringType),
                                         StructField("unique_id", LongType)
                                       ))

    assertThrows[IllegalArgumentException] {
      new UniqueTopKAggregator[Array[Any]](invalidStructType, 3)
    }
  }

  "UniqueTopKAggregator" should "handle merge operations" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    // First batch
    val ir1 = aggregator.prepare(5)
    aggregator.update(ir1, 3)
    aggregator.update(ir1, 8)

    // Second batch
    val ir2 = aggregator.prepare(7)
    aggregator.update(ir2, 1)
    aggregator.update(ir2, 5) // Duplicate

    val merged = aggregator.merge(ir1, ir2)
    val result = aggregator.finalize(merged)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List(8, 7, 5), resultList)
  }

  "UniqueTopKAggregator" should "handle normalization round-trip" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[String](StringType, k)

    val inputs = List("zebra", "apple", "banana", "cherry")
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val originalResult = aggregator.finalize(aggregator.clone(ir))

    val normalized = aggregator.normalize(ir)
    val denormalized = aggregator.denormalize(normalized)
    val roundTripResult = aggregator.finalize(denormalized)

    assertEquals(originalResult, roundTripResult)
  }

  "UniqueTopKAggregator" should "handle empty input gracefully" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val ir = aggregator.prepare(42)
    val result = aggregator.finalize(ir)

    assertEquals(1, result.size())
    assertEquals(42, result.get(0))
  }

  "UniqueTopKAggregator" should "respect k limit" in {
    val k = 2
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)

    assertEquals(k, result.size())
    assertEquals(List(10, 9), result.asScala.toList)
  }

  "UniqueTopKAggregator" should "handle clone operation correctly" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(5, 3, 8, 1)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val cloned = aggregator.clone(ir)
    val originalResult = aggregator.finalize(ir)
    val clonedResult = aggregator.finalize(cloned)

    assertEquals(originalResult, clonedResult)

    // Verify they are independent
    aggregator.update(ir, 10)
    val newOriginalResult = aggregator.finalize(ir)
    val unchangedClonedResult = aggregator.finalize(cloned)

    assertNotEquals(newOriginalResult, unchangedClonedResult)
  }

  "UniqueTopKAggregator" should "handle maxSize limit and merge groups correctly" in {
    val k = 5
    val aggregator = new UniqueTopKAggregator[Int](IntType, k) // maxSize = 2*k = 10

    // Create 10 groups of 10 random numbers between 1-100
    val random = new Random(42) // Fixed seed for reproducible test
    val groups = (1 to 10).map { _ =>
      (1 to 10).map(_ => random.nextInt(100) + 1).toList
    }

    val allNums = groups.flatten
    val top5 = allNums.distinct.sorted(Ordering[Int].reverse).take(k).toList

    // Create intermediate results for each group
    val intermediateResults = groups.map { group =>
      val ir = aggregator.prepare(group.head)
      group.tail.foreach(num => aggregator.update(ir, num))
      ir
    }

    // Merge all groups into one state
    val finalState = intermediateResults.reduce((ir1, ir2) => aggregator.merge(ir1, ir2))

    val result = aggregator.finalize(finalState)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())

    // The result should contain the top 5 values (96-100)
    val expected = top5
    assertEquals(expected, resultList)

    // Verify all results are >= 96
    resultList.foreach { value =>
      assertTrue(s"Value $value should be >= 96", value >= 96)
    }
  }

  "UniqueTopKAggregator" should "respect maxSize constraint during operations" in {
    val k = 3
    val maxSize = 2 * k // Default maxSize
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    // Add more than maxSize unique elements
    val inputs = (1 to 20).toList
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    // The internal state should be limited by maxSize during processing
    // but final result should still be k elements
    val result = aggregator.finalize(ir)

    assertEquals(k, result.size())
    assertEquals(List(20, 19, 18), result.asScala.toList)
  }

  // ===== LAST_SEEN mode tests =====

  "UniqueTopKAggregator with LAST_SEEN mode" should "replace elements with duplicate IDs" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k, None, "LAST_SEEN")

    // Add same ID multiple times - LAST_SEEN should keep the last occurrence
    val inputs = List(5, 3, 8, 5, 3, 5)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    // Should have 3 unique values
    assertEquals(3, result.size())
    assertEquals(List(8, 5, 3), resultList)
  }

  "UniqueTopKAggregator with LAST_SEEN mode and IntType" should "always keep last seen value" in {
    val k = 5
    val aggregator = new UniqueTopKAggregator[Int](IntType, k, None, "LAST_SEEN")

    val inputs = List(1, 2, 1, 3, 2, 1, 4, 3, 2, 1)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(4, result.size()) // 4 unique IDs
    assertEquals(List(4, 3, 2, 1), resultList)
  }

  "UniqueTopKAggregator with LAST_SEEN mode and StructType" should "replace struct when duplicate ID is seen" in {
    val structType = StructType("TestStruct",
                                Array(
                                  StructField("sort_key", StringType),
                                  StructField("unique_id", LongType),
                                  StructField("value", IntType)
                                ))

    val k = 3
    val aggregator = new UniqueTopKAggregator[Array[Any]](structType, k, None, "LAST_SEEN")

    val inputs = List(
      Array("z", 1L, 10),  // ID=1, value=10
      Array("y", 2L, 20),  // ID=2, value=20
      Array("x", 3L, 30),  // ID=3, value=30
      Array("w", 1L, 100), // ID=1 again - should REPLACE with value=100
      Array("v", 2L, 200)  // ID=2 again - should REPLACE with value=200
    )

    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(3, result.size())

    // Check that ID=1 now has value=100 (not 10), and ID=2 has value=200 (not 20)
    val id1Entry = resultList.find(arr => arr(1) == 1L).get
    val id2Entry = resultList.find(arr => arr(1) == 2L).get
    val id3Entry = resultList.find(arr => arr(1) == 3L).get

    assertEquals(100, id1Entry(2)) // Last seen value for ID=1
    assertEquals(200, id2Entry(2)) // Last seen value for ID=2
    assertEquals(30, id3Entry(2))  // Original value for ID=3

    // Check sort keys also updated (last seen)
    assertEquals("w", id1Entry(0))
    assertEquals("v", id2Entry(0))
    assertEquals("x", id3Entry(0))
  }

  "UniqueTopKAggregator with LAST_SEEN mode" should "handle merge operations correctly" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k, None, "LAST_SEEN")

    // First batch
    val ir1 = aggregator.prepare(5)
    aggregator.update(ir1, 3)
    aggregator.update(ir1, 8)

    // Second batch - has duplicate ID=5 which should replace
    val ir2 = aggregator.prepare(7)
    aggregator.update(ir2, 1)
    aggregator.update(ir2, 5) // Duplicate of 5 from first batch

    val merged = aggregator.merge(ir1, ir2)
    val result = aggregator.finalize(merged)
    val resultList = result.asScala.toList

    // When merging, the 5 from ir2 should replace the 5 from ir1
    // We have 5 unique values (8, 7, 5, 3, 1) but k=3 so only top 3
    assertEquals(k, result.size())
    // Top 3 should be 8, 7, 5
    assertEquals(List(8, 7, 5), resultList)
  }

  "UniqueTopKAggregator with FIRST_SEEN mode (default)" should "reject duplicates as before" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k, None, "FIRST_SEEN")

    val inputs = List(5, 3, 8, 5, 3) // Duplicates should be ignored
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(3, result.size())
    assertEquals(List(8, 5, 3), resultList) // First occurrence of each kept
  }

  "UniqueTopKAggregator with LAST_SEEN mode" should "handle normalization round-trip" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k, None, "LAST_SEEN")

    val inputs = List(5, 3, 8, 5, 3) // With duplicates
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val originalResult = aggregator.finalize(aggregator.clone(ir))

    val normalized = aggregator.normalize(ir)
    val denormalized = aggregator.denormalize(normalized)
    val roundTripResult = aggregator.finalize(denormalized)

    assertEquals(originalResult, roundTripResult)
  }

  "UniqueTopKAggregator with LAST_SEEN mode" should "handle clone operation correctly" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k, None, "LAST_SEEN")

    val inputs = List(5, 3, 8, 1)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val cloned = aggregator.clone(ir)
    val originalResult = aggregator.finalize(aggregator.clone(ir)) // Clone before finalizing to preserve state
    val clonedResult = aggregator.finalize(cloned)

    assertEquals(originalResult, clonedResult)

    // Verify they are independent - update original with new element
    aggregator.update(ir, 10) // Add a new larger element
    val newOriginalResult = aggregator.finalize(ir)

    // The cloned state should not be affected
    // Original should now have [10, 8, 5] (top 3 after adding 10)
    assertEquals(List(10, 8, 5), newOriginalResult.asScala.toList)
    // Cloned should still have original [8, 5, 3]
    assertEquals(List(8, 5, 3), clonedResult.asScala.toList)
  }

  "UniqueTopKAggregator with LAST_SEEN mode and LongType" should "replace elements correctly" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Long](LongType, k, None, "LAST_SEEN")

    val inputs = List(100L, 200L, 300L, 100L, 200L)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(3, result.size())
    assertEquals(List(300L, 200L, 100L), resultList)
  }

  "UniqueTopKAggregator with LAST_SEEN mode and StringType" should "replace elements correctly" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[String](StringType, k, None, "LAST_SEEN")

    val inputs = List("apple", "banana", "cherry", "apple", "banana")
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(3, result.size())
    // String comparison by lexicographical order
    assertEquals(List("cherry", "banana", "apple"), resultList)
  }
}
