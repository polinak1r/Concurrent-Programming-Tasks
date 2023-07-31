package day2

import day1.*
import kotlinx.atomicfu.*

// TODO: Copy the code from `FAABasedQueueSimplified`
// TODO: and implement the infinite array on a linked list
// TODO: of fixed-size segments.
class FAABasedQueue<E> : Queue<E> {

    private val head: AtomicRef<Node>
    private val tail: AtomicRef<Node>
    private val enqIdx = atomic(0)
    private val deqIdx = atomic(0)

    init {
        val dummy = Node(0)
        head = atomic(dummy)
        tail = atomic(dummy)
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.value
            val i = enqIdx.getAndIncrement()
            val segmentNode = findSegment(curTail, i / SEGMENT_SIZE)
            tail.compareAndSet(curTail, segmentNode)
            if (segmentNode.segment[i % SEGMENT_SIZE].compareAndSet(null, element)) return
        }
    }

    private fun findSegment(start: Node, id: Int): Node {
        var curNode: Node = start
        while (true) {
            var nextNode: Node? = curNode.next.value
            while (nextNode != null) {
                if (curNode.id == id) return curNode
                curNode = nextNode
                nextNode = curNode.next.value
            }
            val newNode = Node(curNode.id+1)
            curNode.next.compareAndSet(null, newNode)
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (deqIdx.value >= enqIdx.value) return null
            val curHead = head.value
            val i = deqIdx.getAndIncrement()
            val segmentNode = findSegment(curHead, i / SEGMENT_SIZE)
            head.compareAndSet(curHead, segmentNode)
            if (segmentNode.segment[i % SEGMENT_SIZE].compareAndSet(null, POISONED)) {
                continue
            }
            return segmentNode.segment[i % SEGMENT_SIZE].value as E
        }
    }

    private class Node(
        val id: Int
    ) {
        val segment = atomicArrayOfNulls<Any?>(SEGMENT_SIZE)
        val next = atomic<Node?>(null)
    }
}

// TODO: poison cells with this value.
private val POISONED = Any()
private val SEGMENT_SIZE = 4