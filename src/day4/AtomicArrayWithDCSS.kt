package day4

import kotlinx.atomicfu.*

// This implementation never stores `null` values.
class AtomicArrayWithDCSS<E : Any>(size: Int, initialValue: E) {
    private val array = atomicArrayOfNulls<Any>(size)

    init {
        for (i in 0 until size) {
            array[i].value = initialValue
        }
    }

    fun get(index: Int): E? {
        return when (val curValue = array[index].value) {
            is AtomicArrayWithDCSS<*>.DCSSDescriptor<*> -> {
                curValue.applyOperation()
                if (curValue.status.value == Status.SUCCESS) curValue.update1 as E?
                else curValue.expected1 as E?
            }

            else -> {
                curValue as E?
            }
        }
    }

    fun cas(index: Int, expected: E?, update: E?): Boolean {
        // TODO: the cell can store a descriptor
        while (true) {
            when(val curValue = array[index].value) {
                is AtomicArrayWithDCSS<*>.DCSSDescriptor<*> -> {
                    curValue.applyOperation()
                    continue
                }

                expected -> {
                    if (array[index].compareAndSet(expected, update)) {
                        return true
                    }
                    continue
                }

                else -> {
                    return false
                }
            }
        }
    }

    fun dcss(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        // TODO This implementation is not linearizable!
        // TODO Store a DCSS descriptor in array[index1].

        while (true) {
            when (val curValue = array[index1].value) {
                is AtomicArrayWithDCSS<*>.DCSSDescriptor<*> -> {
                    curValue.applyOperation()
                    continue
                }
                else -> {
                    val descriptor = DCSSDescriptor(index1, expected1, update1, index2, expected2)
                    if (array[index1].compareAndSet(expected1, descriptor)) {
                        descriptor.applyOperation()
                        return descriptor.status.value == Status.SUCCESS
                    }
                    val curValueAfterCompare = array[index1].value
                    if (curValueAfterCompare is AtomicArrayWithDCSS<*>.DCSSDescriptor<*> || curValueAfterCompare == expected1) {
                        continue
                    }
                    else return false
                }
            }
        }
    }

    private inner class DCSSDescriptor<V>(
        val index1: Int, val expected1: E?, val update1: E?,
        val index2: Int, val expected2: V?
    ) {
        val status = atomic(Status.UNDECIDED)
        fun applyOperation() {
            while (true) {

                val secondValue = when (val curValue = array[index2].value) {
                    is AtomicArrayWithDCSS<*>.DCSSDescriptor<*> -> {
                        if (curValue.status.value == Status.SUCCESS) curValue.update1
                        else curValue.expected1
                    }
                    else -> {
                        curValue
                    }
                }

                when (secondValue) {
                    expected2 -> {
                        status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
                    }
                    else -> {
                        status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                    }
                }

                when (status.value) {
                    Status.UNDECIDED -> {
                        status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                        continue
                    }
                    Status.FAILED -> break
                    Status.SUCCESS -> {}
                }

                array[index1].compareAndSet(this, update1)
                if (array[index1].value == this) continue

                break
            }

            if (status.value == Status.FAILED) {
                array[index1].compareAndSet(this, expected1)
            }
        }
    }

    private enum class Status {
        UNDECIDED, FAILED, SUCCESS
    }
}