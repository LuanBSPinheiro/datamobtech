package com.example.datamobtech

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

fun <T, R> Flow<T>.parallelMapOrdered(
    concurrency: Int,
    transform: suspend (T) -> R
): Flow<R> {
    require(concurrency > 0) { "concurrency must be > 0, but was $concurrency" }

    return channelFlow {
        val semaphore = Semaphore(concurrency)
        val queue = Channel<Deferred<R>>(Channel.BUFFERED)

        val consumerJob = async(start = CoroutineStart.UNDISPATCHED) {
            for (deferred in queue) {
                send(deferred.await())
            }
        }

        try {
            collect { item ->
                val deferred = async {
                    semaphore.withPermit {
                        transform(item)
                    }
                }
                queue.send(deferred)
            }
        } finally {
            queue.close()
        }

        consumerJob.await()
    }
}