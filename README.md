## Problemas encontrados y soluciones

### Coordinación entre instancias de sum

**Problema**  
Las instancias de `sum` consumen de la misma working queue. Cuando un cliente enviaba un EOF, solo una lo recibía y el resto no se enteraba de que el flujo había terminado.

Esto hacía que algunas instancias no enviaran sus resultados parciales a `aggregation`, dejando resultados incompletos.

**Solución**  
Se agregó un exchange de control (`SUM_CONTROL_EXCHANGE`).

Cuando una instancia de `sum` recibe un EOF:
- lo procesa
- publica un mensaje de control (`MessageTypeSumEOF`) en el exchange

El resto de las instancias están suscritas (en una goroutine), así que también reciben ese EOF y ejecutan su lógica de cierre.

**Resultado**  
Todas las instancias quedan sincronizadas para un mismo `task` y cada una envía sus parciales, evitando que falten datos en `aggregation`.

---

### Distribución consistente hacia aggregation

**Problema**  
La misma fruta podía terminar en distintos aggregators, generando tops inconsistentes.

**Solución**  
Se implementó un particionamiento determinístico en `sum`:

```go
partition = hash(fruit) % aggregationCount
```
Así, cada fruta siempre cae en el mismo aggregator para un `taskId`.

---

### Coordinación en join

**Problema**  
`join` no puede emitir el resultado final hasta recibir todos los EOF de los aggregators.

**Solución**  
Se cuenta cuántos EOF llegaron por `taskId` y se compara con `aggregationAmount`.  
Solo cuando están todos:

- se arma el resultado final
- se envía el top K

Es la una idea parecida a la de coordinación que en `sum`, pero aplicada al final del pipeline.

---
    
### Flush prematuro en sum

**Problema**  
Un `sum` podía recibir el EOF antes de terminar de procesar mensajes pendientes, lo que llevaba a resultados incompletos.

Además hay un caso borde importante:  
el EOF puede llegar justo después de que el último mensaje hace `pendingMessages--`, o incluso mientras todavía hay procesamiento en paralelo.

**Solución**  
Se maneja estado por `taskId`:

- `pendingMessages`: mensajes en procesamiento
- `eofReceived`: si ya llegó el EOF
- `flushDone`: evita ejecutar flush más de una vez

El flush se hace solo cuando:

- llegó el EOF
- no quedan mensajes pendientes

La clave es no depender del orden de llegada, sino del estado.

Se re-evalúa la condición de flush en dos momentos:

- cuando termina un mensaje (`pendingMessages--`)
- cuando llega el EOF (`eofReceived = true`)

Esto cubre el caso donde el EOF llega justo después del último mensaje.  
Si todavía no había llegado el EOF, no pasa nada. Cuando llega, se vuelve a evaluar y flushea correctamente.

**Resultado**

- no hay flush prematuro
- no quedan tareas colgadas
- el resultado se emite recién cuando todo fue procesado  

