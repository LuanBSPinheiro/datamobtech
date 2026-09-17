package com.example.datamobtech

import org.junit.Assert.assertEquals
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.Test

class ParallelMapOrderedTest {

    @Test(expected = IllegalArgumentException::class)
    fun `deve lancar IllegalArgumentException quando concurrency for menor ou igual a zero`() = runTest {
        emptyFlow<Int>()
            .parallelMapOrdered(concurrency = 0) { it * 2 }
            .toList()
    }

    @Test
    fun `deve emitir os itens transformados na ordem correta do upstream`() = runTest {
        val input = (1..5).toList()

        val result = input.asFlow()
            .parallelMapOrdered(concurrency = 2) { it * 10 }
            .toList()

        assertEquals(listOf(10, 20, 30, 40, 50), result)
    }
}