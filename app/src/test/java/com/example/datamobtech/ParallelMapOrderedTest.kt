package com.example.datamobtech

import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.Test

class ParallelMapOrderedTest {

    @Test(expected = IllegalArgumentException::class)
    fun `deve lançar IllegalArgumentException quando concurrency for menor ou igual a zero`() = runTest {
        emptyFlow<Int>()
            .parallelMapOrdered(concurrency = 0) { it * 2 }
            .toList()
    }
}