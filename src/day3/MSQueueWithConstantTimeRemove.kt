@file:Suppress("DuplicatedCode", "FoldInitializerAndIfToElvis")

package day3

import kotlinx.atomicfu.*

class MSQueueWithConstantTimeRemove<E> : QueueWithRemove<E> {
    private val head: AtomicRef<Node<E>>
    private val tail: AtomicRef<Node<E>>

    init {
        val dummy = Node<E>(element = null, prev = null)
        head = atomic(dummy)
        tail = atomic(dummy)
    }

    override fun enqueue(element: E) {
        // TODO: When adding a new node, check whether
        // TODO: the previous tail is logically removed.
        // TODO: If so, remove it physically from the linked list.

        while (true) {
            val curTail = tail.value
            val node = Node(element, curTail)

            if (curTail.next.compareAndSet(null, node)) {
                if (tail.compareAndSet(curTail, node)) {
                    if (curTail.extractedOrRemoved && curTail != head.value) curTail.removePhysic()
                }
                return
            } else {
                val next = curTail.next.value ?: continue
                tail.compareAndSet(curTail, next)
                if (curTail.extractedOrRemoved && curTail != head.value) curTail.removePhysic()
            }
        }
    }

    override fun dequeue(): E? {
        // TODO: After moving the `head` pointer forward,
        // TODO: mark the node that contains the extracting
        // TODO: element as "extracted or removed", restarting
        // TODO: the operation if this node has already been removed.

        while (true) {
            val curHead = head.value
            val curHeadNext = curHead.next.value ?: return null

            if (head.compareAndSet(curHead, curHeadNext)) {
                curHeadNext.prev.getAndSet(null)
                if (curHeadNext.markExtractedOrRemoved()) {
                    return curHeadNext.element
                }
            }
        }
    }

    override fun remove(element: E): Boolean {
        // Traverse the linked list, searching the specified
        // element. Try to remove the corresponding node if found.
        // DO NOT CHANGE THIS CODE.
        var node = head.value
        while (true) {
            val next = node.next.value
            if (next == null) return false
            node = next
            if (node.element == element && node.remove()) return true
        }
    }

    /**
     * This is an internal function for tests.
     * DO NOT CHANGE THIS CODE.
     */
    override fun checkNoRemovedElements() {
        check(head.value.prev.value == null) {
            "`head.prev` must be null"
        }
        check(tail.value.next.value == null) {
            "tail.next must be null"
        }
        // Traverse the linked list
        var node = head.value
        while (true) {
            if (node !== head.value && node !== tail.value) {
                check(!node.extractedOrRemoved) {
                    "Removed node with element ${node.element} found in the middle of the queue"
                }
            }
            val nodeNext = node.next.value
            // Is this the end of the linked list?
            if (nodeNext == null) break
            // Is next.prev points to the current node?
            val nodeNextPrev = nodeNext.prev.value
            check(nodeNextPrev != null) {
                "The `prev` pointer of node with element ${nodeNext.element} is `null`, while the node is in the middle of the queue"
            }
            check(nodeNextPrev == node) {
                "node.next.prev != node; `node` contains ${node.element}, `node.next` contains ${nodeNext.element}"
            }
            // Process the next node.
            node = nodeNext
        }
    }

    /**
     * This is a string representation of the data structure,
     * you see it in Lincheck tests when they fail.
     * DO NOT CHANGE THIS CODE.
     */
    override fun toString(): String {
        // Choose the leftmost node.
        var node = head.value
        if (tail.value.next.value === node) {
            node = tail.value
        }
        // Traverse the linked list.
        val nodes = arrayListOf<String>()
        while (true) {
            nodes += (if (head.value === node) "HEAD = " else "") +
                    (if (tail.value === node) "TAIL = " else "") +
                    "<${node.element}" +
                    (if (node.extractedOrRemoved) ", extractedOrRemoved" else "") +
                    ">"
            // Process the next node.
            node = node.next.value ?: break
        }
        return nodes.joinToString(", ")
    }

    private class Node<E>(
        var element: E?,
        prev: Node<E>?
    ) {
        val next = atomic<Node<E>?>(null)
        val prev = atomic(prev)

        /**
         * TODO: Both [dequeue] and [remove] should mark
         * TODO: nodes as "extracted or removed".
         */
        private val _extractedOrRemoved = atomic(false)
        val extractedOrRemoved get() = _extractedOrRemoved.value

        fun markExtractedOrRemoved(): Boolean = _extractedOrRemoved.compareAndSet(expect = false, update = true)

        /**
         * Removes this node from the queue structure.
         * Returns `true` if this node was successfully
         * removed, or `false` if it has already been
         * removed by [remove] or extracted by [dequeue].
         */
        fun remove(): Boolean {
            // TODO: As in the previous task, the removal procedure is split into two phases.
            // TODO: First, you need to mark the node as "extracted or removed".
            // TODO: On success, this node is logically removed, and the
            // TODO: operation should return `true` at the end.
            // TODO: In the second phase, the node should be removed
            // TODO: physically, updating the `next` field of the previous
            // TODO: node to `this.next.value`.
            // TODO: In this task, you have to maintain the `prev` pointer,
            // TODO: which references the previous node. Thus, the `remove()`
            // TODO: complexity becomes O(1) under no contention.
            // TODO: Do not remove physical head and tail of the linked list;
            // TODO: it is totally fine to have a bounded number of removed nodes
            // TODO: in the linked list, especially when it significantly simplifies
            // TODO: the algorithm.

            if (markExtractedOrRemoved()) {

                removePhysic()

                return true
            }
            return false
        }

        fun removePhysic() {
            if (this.next.value == null) return
            if (this.prev.value == null) return
            //if (this == head.value) return

            val prevNode = findPrev() ?: return
            val nextNode = next.value
            prevNode.next.getAndSet(nextNode)
            if (nextNode != null) {
                while (true) {
                    val curNextNode = nextNode.prev.value ?: break
                    if (nextNode.prev.compareAndSet(curNextNode, prevNode)) break
                }
            }
            if (prevNode.extractedOrRemoved) prevNode.removePhysic()
            if (nextNode != null && nextNode.extractedOrRemoved) nextNode.removePhysic()
        }

        private fun findPrev(): Node<E>? {
            return prev.value
        }
    }
}