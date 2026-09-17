package com.example.datamobtech

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import org.junit.Assert.assertEquals
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.Collections
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

    @Test
    fun `item lento no inicio nao deve bloquear a execucao concorrente dos seguintes`() = runTest {
        val concurrency = 4
        val slowItemGate = CompletableDeferred<Unit>()
        val executedTransforms = Collections.synchronizedList(mutableListOf<Int>())

        val job = launch {
            (1..4).asFlow()
                .parallelMapOrdered(concurrency = concurrency) { item ->
                    if (item == 1) {
                        slowItemGate.await() // Trava o primeiro item
                    }
                    executedTransforms.add(item)
                    item
                }
                .collect { }
        }

        testScheduler.advanceUntilIdle()

        // Mesmo com o item 1 suspenso no gate, os itens 2, 3 e 4 devem ter finalizado o transform
        assertTrue(
            "Itens 2, 3 e 4 deveriam ter rodado o transform concorrentemente mesmo com o item 1 travado",
            executedTransforms.containsAll(listOf(2, 3, 4))
        )
        assertTrue(
            "Item 1 não deveria ter terminado o transform ainda",
            !executedTransforms.contains(1)
        )

        // Destrava o item 1 para o fluxo poder completar
        slowItemGate.complete(Unit)
        testScheduler.advanceUntilIdle()
        job.join()
    }
}