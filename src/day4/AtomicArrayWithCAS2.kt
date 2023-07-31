@file:Suppress("DuplicatedCode")

package day4

import kotlinx.atomicfu.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = atomicArrayOfNulls<Any>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i].value = initialValue
        }
    }

    fun get(index: Int): E? {
        // TODO: the cell can store a descriptor
        while (true) {
            return when (val currentValue = array[index].value) {
                is AtomicArrayWithCAS2<*>.CAS2Descriptor -> {
                    if (currentValue.status.value == Status.SUCCESS) {
                        if (index == currentValue.index1) currentValue.update1 as E?
                        else currentValue.update2 as E?
                    } else {
                        if (index == currentValue.index1) currentValue.expected1 as E?
                        else currentValue.expected2 as E?
                    }
                }

                is AtomicArrayWithCAS2<*>.DCSSDescriptor<*> -> {
                    currentValue.applyOperation()
                    val resultOfDCSS = if (currentValue.status.value == Status.SUCCESS) currentValue.update1
                    else currentValue.expected1
                    if (resultOfDCSS is AtomicArrayWithCAS2<*>.CAS2Descriptor) continue
                    resultOfDCSS as E?
                }

                else -> {
                    currentValue as E?
                }
            }
        }
    }

    fun cas(index: Int, expected: E?, update: E?): Boolean {
        // TODO: the cell can store a descriptor
        while (true) {
            when(val currentValue = array[index].value) {
                is AtomicArrayWithCAS2<*>.DCSSDescriptor<*> -> {
                    currentValue.applyOperation()
                    continue
                }

                is AtomicArrayWithCAS2<*>.CAS2Descriptor -> {
                    currentValue.applyOperation()
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

    fun cas2(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?, update2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        // TODO: this implementation is not linearizable,
        // TODO: Store a CAS2 descriptor in array[index1].

        while (true) {
            val firstValue = array[index1].value
            val secondValue = array[index2].value

            if (firstValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                firstValue.applyOperation()
                continue
            }
            if (firstValue is AtomicArrayWithCAS2<*>.DCSSDescriptor<*>) {
                firstValue.applyOperation()
                continue
            }
            if (secondValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                secondValue.applyOperation()
                continue
            }
            if (secondValue is AtomicArrayWithCAS2<*>.DCSSDescriptor<*>) {
                secondValue.applyOperation()
                continue
            }

            if (firstValue as E? != expected1 || secondValue as E? != expected2) return false

            val descriptor = if (index1 < index2) {
                CAS2Descriptor(index1, expected1, update1, index2, expected2, update2)
            }
            else {
                CAS2Descriptor(index2, expected2, update2, index1, expected1, update1)
            }
            val minInd = if (index1 < index2) index1 else index2
            val minIndValue = if (index1 < index2) expected1 else expected2
            if (array[minInd].compareAndSet(minIndValue, descriptor)) {
                descriptor.applyOperation()
                return descriptor.status.value == Status.SUCCESS
            }
        }
    }

    private inner class CAS2Descriptor(
        val index1: Int, val expected1: E?, val update1: E?,
        val index2: Int, val expected2: E?, val update2: E?
    ) {
        val status = atomic(Status.UNDECIDED)

        fun applyOperation() {

            while (true) {

                val isSuccess = dcss(index2, expected2, this, this, Status.UNDECIDED)
                //val isSuccess = array[index2].compareAndSet(expected2, this)

                if (isSuccess) status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)

                else when (val secondValue = array[index2].value) {
                    this -> {
                        status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
                    }
                    is AtomicArrayWithCAS2<*>.CAS2Descriptor -> {
                        secondValue.applyOperation()
                        continue
                    }
                    is AtomicArrayWithCAS2<*>.DCSSDescriptor<*> -> {
                        secondValue.applyOperation()
                        continue
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

                array[index2].compareAndSet(this, update2)
                if (array[index2].value == this) continue

                break
            }

            if (status.value == Status.FAILED) {
                array[index2].compareAndSet(this, expected2)
                array[index1].compareAndSet(this, expected1)
            }
        }
    }

    private fun <V: Enum<V>> dcss(
        index1: Int, expected1: Any?, update1: Any?,
        caS2Descriptor: CAS2Descriptor, expected2: V
    ): Boolean {
        // TODO This implementation is not linearizable!
        // TODO Store a DCSS descriptor in array[index1].

        while (true) {
            when (val currentValue = array[index1].value) {
                caS2Descriptor -> {
                    return true
                }
                is AtomicArrayWithCAS2<*>.DCSSDescriptor<*> -> {
                    currentValue.applyOperation()
                    continue
                }
                is AtomicArrayWithCAS2<*>.CAS2Descriptor -> {
                    currentValue.applyOperation()
                    continue
                }
                else -> {
                    val descriptor = DCSSDescriptor(index1, expected1, update1, caS2Descriptor, expected2)
                    if (array[index1].compareAndSet(expected1, descriptor)) {
                        descriptor.applyOperation()
                        return descriptor.status.value == Status.SUCCESS
                    }
                    val curValueAfterCompare = array[index1].value
                    if (
                        curValueAfterCompare is AtomicArrayWithCAS2<*>.DCSSDescriptor<*> ||
                        curValueAfterCompare is AtomicArrayWithCAS2<*>.CAS2Descriptor ||
                        curValueAfterCompare == expected1) {
                        continue
                    }
                    else return false
                }
            }
        }
    }

    private inner class DCSSDescriptor<E: Enum<E>>(
        val index1: Int, val expected1: Any?, val update1: Any?,
        val caS2Descriptor: CAS2Descriptor, val expected2: E?
    ) {
        val status = atomic(Status.UNDECIDED)
        fun applyOperation() {
            while (true) {

                when (val resultValue = array[index1].value) {
                    this -> {
                    }
                    is AtomicArrayWithCAS2<*>.DCSSDescriptor<*> -> {
                        resultValue.applyOperation()
                        continue
                    }
                    is AtomicArrayWithCAS2<*>.CAS2Descriptor -> {
                        resultValue.applyOperation()
                        continue
                    }
                    else -> {
                        status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                        break
                    }
                }

                when (caS2Descriptor.status.value) {
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