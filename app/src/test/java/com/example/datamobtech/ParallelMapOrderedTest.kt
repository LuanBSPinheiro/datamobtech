package com.example.datamobtech

import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

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

    @Test
    fun `nunca deve exceder o limite de concorrencia configurado`() = runTest {
        val concurrencyLimit = 3
        val activeWorkers = AtomicInteger(0)
        val maxObservedConcurrency = AtomicInteger(0)

        val result = (1..20).asFlow()
            .parallelMapOrdered(concurrency = concurrencyLimit) { item ->
                val current = activeWorkers.incrementAndGet()
                maxObservedConcurrency.updateAndGet { max -> maxOf(max, current) }

                // Simula trabalho assíncrono para dar tempo de outras coroutines entrarem
                delay(50.milliseconds)

                activeWorkers.decrementAndGet()
                item
            }
            .toList()

        assertEquals((1..20).toList(), result)
        assertTrue(
            "Concorrência observada (${maxObservedConcurrency.get()}) não pode ultrapassar o limite de $concurrencyLimit",
            maxObservedConcurrency.get() <= concurrencyLimit
        )
    }
}