package ai.chronon.aggregator.base

import java.util
import java.util.Comparator

object UniqueOrderByLimit {

  def initState[T, OrderType]: State[T, OrderType] =
    State(new util.ArrayList[T](), new util.HashSet[Long](), null.asInstanceOf[OrderType])

  case class State[T, OrderType](elems: java.util.ArrayList[T],
                                 ids: java.util.HashSet[Long],
                                 var orderWaterMark: OrderType)

  // UniqueTopKAggregator will create this Operator from input args and call the public methods in its implementation
  case class Operator[T, OrderType: Ordering](getOrderKey: T => OrderType,
                                              getId: T => Long,
                                              k: Int,
                                              maxSize: Int,
                                              topK: Boolean = true) {

    private val ordering = implicitly[Ordering[OrderType]]

    // to be used in "finalize" of the aggregator
    def sortAndPrune(state: State[T, OrderType]): Unit = {

      sort(state)
      val elems = state.elems

      while (elems.size > k) {
        val removed = elems.remove(elems.size - 1)
        state.ids.remove(getId(removed))
      }

      if (elems.size > 0) {
        state.orderWaterMark = getOrderKey(elems.get(elems.size - 1))
      }
    }

    // to be used to impl "denormalize" of the aggregator
    def buildStateFromElems(elems: java.util.ArrayList[T]): State[T, OrderType] = {

      val state: State[T, OrderType] = UniqueOrderByLimit.initState[T, OrderType]
      val it = elems.iterator()

      while (it.hasNext) {
        insert(it.next(), state)
      }

      state
    }

    def insert(elem: T, state: State[T, OrderType]): Unit = {

      // dedup first
      if (state.ids.contains(getId(elem))) return

      val orderKey = getOrderKey(elem)

      if (topK) {
        if (state.elems.size() < k) {

          state.orderWaterMark = if (state.orderWaterMark == null || ordering.lt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          state.elems.add(elem)
          state.ids.add(getId(elem))

        } else if (ordering.gt(orderKey, state.orderWaterMark)) {

          state.elems.add(elem)
          state.ids.add(getId(elem))

        }
      } else {
        if (state.elems.size() < k) {
          // keep min
          state.orderWaterMark = if (state.orderWaterMark == null || ordering.gt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          state.elems.add(elem)
          state.ids.add(getId(elem))

        } else if (ordering.lt(orderKey, state.orderWaterMark)) {

          state.elems.add(elem)
          state.ids.add(getId(elem))

        }
      }

      if (state.elems.size() > maxSize) {
        sortAndPrune(state)
      }
    }

    private def sort(state: State[T, OrderType]): Unit = {

      state.elems.sort(new Comparator[T] {
        override def compare(o1: T, o2: T): Int = {
          val o1Key = getOrderKey(o1)
          val o2Key = getOrderKey(o2)

          // sort desc when topK or sort asc when bottomK
          if (topK) {
            // descending
            ordering.compare(o2Key, o1Key)
          } else {
            // ascending
            ordering.compare(o1Key, o2Key)
          }
        }
      })
    }
  }

  // Lenient (LAST_SEEN) implementation that allows replacing elements with same ID
  def initLenientState[T, OrderType]: LenientState[T, OrderType] =
    LenientState(new util.ArrayList[T](),
                 new util.HashMap[Long, Int](),
                 null.asInstanceOf[OrderType],
                 indicesDirty = false)

  case class LenientState[T, OrderType](elems: java.util.ArrayList[T],
                                        idToIndex: java.util.HashMap[Long, Int],
                                        var orderWaterMark: OrderType,
                                        var indicesDirty: Boolean)

  // LenientOperator implements LAST_SEEN behavior: always replace when duplicate ID is seen
  case class LenientOperator[T, OrderType: Ordering](getOrderKey: T => OrderType,
                                                     getId: T => Long,
                                                     k: Int,
                                                     maxSize: Int,
                                                     topK: Boolean = true) {

    private val ordering = implicitly[Ordering[OrderType]]

    // Rebuild the idToIndex map after sorting (indices have changed)
    private def rebuildIndexMap(state: LenientState[T, OrderType]): Unit = {
      state.idToIndex.clear()
      var i = 0
      while (i < state.elems.size()) {
        val elem = state.elems.get(i)
        state.idToIndex.put(getId(elem), i)
        i += 1
      }
      state.indicesDirty = false
    }

    // to be used in "finalize" of the aggregator
    def sortAndPrune(state: LenientState[T, OrderType]): Unit = {
      sort(state)
      val elems = state.elems

      // Remove elements beyond k
      while (elems.size > k) {
        val removed = elems.remove(elems.size - 1)
        state.idToIndex.remove(getId(removed))
      }

      if (elems.size > 0) {
        state.orderWaterMark = getOrderKey(elems.get(elems.size - 1))
      }

      // Lazy rebuild: only rebuild if we actually sorted and pruned
      if (state.indicesDirty) {
        rebuildIndexMap(state)
      }
    }

    // to be used to impl "denormalize" of the aggregator
    def buildStateFromElems(elems: java.util.ArrayList[T]): LenientState[T, OrderType] = {
      val state: LenientState[T, OrderType] = UniqueOrderByLimit.initLenientState[T, OrderType]
      val it = elems.iterator()

      while (it.hasNext) {
        insert(it.next(), state)
      }

      state
    }

    def insert(elem: T, state: LenientState[T, OrderType]): Unit = {
      val elemId = getId(elem)
      val orderKey = getOrderKey(elem)

      // LAST_SEEN behavior: if ID exists, replace the element at that index
      if (state.idToIndex.containsKey(elemId)) {
        val existingIndex = state.idToIndex.get(elemId)
        state.elems.set(existingIndex, elem)
        // Note: index doesn't change, so no need to update map or set dirty flag
        return
      }

      // New element - same logic as strict version but track index instead of just ID
      if (topK) {
        if (state.elems.size() < k) {
          state.orderWaterMark = if (state.orderWaterMark == null || ordering.lt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          val index = state.elems.size()
          state.elems.add(elem)
          state.idToIndex.put(elemId, index)

        } else if (ordering.gt(orderKey, state.orderWaterMark)) {
          val index = state.elems.size()
          state.elems.add(elem)
          state.idToIndex.put(elemId, index)
        }
      } else {
        if (state.elems.size() < k) {
          state.orderWaterMark = if (state.orderWaterMark == null || ordering.gt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          val index = state.elems.size()
          state.elems.add(elem)
          state.idToIndex.put(elemId, index)

        } else if (ordering.lt(orderKey, state.orderWaterMark)) {
          val index = state.elems.size()
          state.elems.add(elem)
          state.idToIndex.put(elemId, index)
        }
      }

      if (state.elems.size() > maxSize) {
        state.indicesDirty = true
        sortAndPrune(state)
      }
    }

    private def sort(state: LenientState[T, OrderType]): Unit = {
      state.elems.sort(new Comparator[T] {
        override def compare(o1: T, o2: T): Int = {
          val o1Key = getOrderKey(o1)
          val o2Key = getOrderKey(o2)

          if (topK) {
            ordering.compare(o2Key, o1Key) // descending
          } else {
            ordering.compare(o1Key, o2Key) // ascending
          }
        }
      })
    }
  }

}
