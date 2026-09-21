# Decisões Técnicas — Operador `parallelMapOrdered` (Kotlin Coroutines)

### 1. Separação entre execução paralela (`Semaphore`) e ordem de entrega (`Channel<Deferred>`)

* **O problema:** A regra proibia operadores prontos de alta ordem (`flatMapMerge`, `flatMapConcat`, etc.) e exigia que um item lento logo no começo não travasse o processamento dos próximos itens até o limite de concorrência.
* **A solução:**
  Usei `channelFlow` dividindo a responsabilidade em duas partes:
    1. **Produtor:** Itera no upstream disparando uma coroutine via `async` para cada item. Esse `async` pega uma permissão no `Semaphore(concurrency)` antes de rodar o `transform`. O ponteiro desse `Deferred` vai direto pra uma fila (`Channel<Deferred<R>>`).
    2. **Consumidor ordenado:** Uma coroutine separada lê os `Deferred` estritamente na ordem da fila, dá `await()` e emite para o coletor via `send()`.
* **Trade-off e o cenário com 10k itens:**
  Se o primeiro item travar e tivermos 10.000 itens chegando:
    - Os próximos itens até o limite da concorrência ($P - 1$) rodam em paralelo e terminam seus `async`, ficando prontos na fila.
    - A partir do item $P$, o semáforo esgota as permissões e o upstream suspende naturalmente.
    - **Memória:** Não há risco de estouro de memória (`OOM`). A fila segura no máximo referências a instâncias de `Deferred` e a quantidade de trabalho ativo/alocado fica limitada ao número de permissões do semáforo.

---

### 2. Concorrência estruturada para garantir Fail-Fast real

* **O problema:** Se qualquer requisição falhar, precisamos derrubar na hora todas as outras que ainda estão rodando e devolver o erro original direto pro chamador, sem mascarar com `JobCancellationException` ou wrappers genéricos.
* **A solução:**
  Todas as coroutines disparadas (`async` dos workers e o consumidor da fila) rodam sob o mesmo `ProducerScope` do `channelFlow`. Por padrão de concorrência estruturada do Kotlin, se um filho quebra com exceção, o escopo pai é cancelado imediatamente, propagando o cancelamento em cascata para todos os workers irmãos cooperativos e re-lançando o erro original limpo.
* **Trade-off:**
  A escolha foi por fail-fast estrito: o pipeline não tenta continuar com os itens restantes se um falhar. Se a regra de negócio do app exigisse tolerância a falhas parciais (ex.: tentar 3 vezes ou ignorar erro 404), isso precisaria ser tratado dentro da lambda de `transform`.

---

### 3. Cancelamento ponta a ponta quando o coletor morre

* **O problema:** Quando a tela é fechada ou a `ViewModel` é limpa no Android, requisições HTTP e operações em andamento precisam ser canceladas imediatamente para economizar rede e bateria.
* **A solução:**
  Como o ciclo de vida do `channelFlow` está diretamente preso ao `Job` de quem está coletando, no momento em que o coletor é cancelado, o escopo fecha e cancela todos os jobs filhos. Como usamos pontos de suspensão nativos (`withPermit`, `delay`, `send/receive`), o cancelamento é cooperativo e quase instantâneo.
* **Trade-off:**
  Para isso funcionar na prática, o `transform` precisa ser cooperativo com cancelamento (usar funções de suspensão reais como Ktor/Retrofit com suspend ou checar `isActive`). Se alguém enfiar código bloqueante de thread síncrona pura sem checagem de cancelamento, a thread não vai parar até terminar.

---

### Metodologia (TDD)
O desenvolvimento seguiu rigorosamente o fluxo de TDD (Red -> Green -> Refactor):
1. Teste de contrato de entrada (`concurrency > 0`);
2. Garantia de entrega ordenada com `channelFlow` e fila de `Deferred`;
3. Limite estrito de paralelismo com `Semaphore`;
4. Validação de que item lento na cabeça não bloqueia a execução paralela dos seguintes;
5. Fail-fast e propagação limpa da exceção de domínio;
6. Cancelamento cooperativo pelo coletor;
7. Teste de carga obrigatório: 1.000 itens com latência randômica e concorrência limitada a 8.