package com.example.datamobtech

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow

fun <T, R> Flow<T>.parallelMapOrdered(
    concurrency: Int,
    transform: suspend (T) -> R
): Flow<R> {
    require(concurrency > 0) { "concurrency must be > 0, but was $concurrency" }
    return emptyFlow()
}