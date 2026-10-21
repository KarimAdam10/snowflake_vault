# Mejoras y consideraciones futuras

Documento vivo: cada punto es una limitación conocida o una evolución posible, con su motivo y el siguiente paso. Al final del trabajo servirá de base para las diapositivas de «límites y evolución».

**Plantilla para añadir un punto:** situación actual · por qué importa · opciones · recomendación.

---

## 1. Detección de borrados en entidades `snapshot`

**Situación actual.** El Raw Vault solo inserta claves y versiones nuevas. Si una clave desaparece de un fichero completo (por ejemplo, `customer` pasa de 20 a 19 clientes), el vault no se entera: el hub y el satélite conservan la clave, y `dim_customer` la sigue mostrando como vigente (`is_current = true`, `valid_to` abierto). No falla nada y ninguna prueba salta (las de «coincide con staging» solo cubren `ref_nation` y `ref_region`, y en un solo sentido).

**Por qué importa.** Un registro dado de baja en origen seguiría contando como activo en el análisis. En los dos lotes de la prueba no desaparece ninguna clave, así que el caso no se da en los datos, pero sí en uso real.

**Por qué el vault no puede decidirlo solo.** La ausencia de una clave solo significa «borrado» si el fichero es completo (`snapshot`). En un fichero parcial (`incremental`) no significa nada. Ese es el papel natural de `load.type`, hoy sin efecto sobre el modelo (solo falla un `snapshot` con 0 filas).

**Opciones**

| Opción | Cómo | Pros y contras |
|---|---|---|
| A. Comparación en el Business Vault o en la dimensión | Vista que cruza `hub_*` con `stg_*`: la clave ausente del último lote se marca como baja | Sin tablas nuevas ni historia de la baja. Solo vale porque landing conserva el último lote completo |
| B. Satélite de estado a medida | Modelo SQL propio alimentado en cada carga de las entidades `snapshot`: filas «activo», «borrado» y «reaparece» | Conserva el historial de bajas. Es código propio, fuera de AutomateDV |
| C. XTS de AutomateDV | Registra los hashdiffs vistos en cada carga; la ausencia se deduce | Usa el paquete, pero es más complejo de lo necesario. Sin probar |

AutomateDV no trae un satélite de estado para hubs. Su `eff_sat` está pensado para la vigencia de relaciones (links) y exige una clave conductora.

**Recomendación.** Si se aborda: A como paso mínimo; B como evolución real. Siempre solo para entidades con `load.type: snapshot`, y la dimensión leería el estado para cerrar `valid_to` o marcar la baja.

**Pendiente de comprobar.** Que el lote 202601 no pierde ninguna clave respecto al 202512 en las seis entidades `snapshot`.

---

## 2. Nuevos tipos de origen y de formato

**Situación actual.** El origen de los lotes se declara en `metadata/ingestion/source.yml` con un `type` (hoy solo `local_folder`: una carpeta `data/<periodo>/` con un CSV por entidad). El cargador elige el extractor en el registro `EXTRACTORS` de `landing/loader.py`. Un extractor son tres funciones (`periods`, `location` y `fetch`) y su único contrato es dejar los CSV del periodo en un directorio; contratos, carga y dbt no saben de dónde vienen. El extractor local no copia ni descomprime nada.

**Qué falta para otros orígenes.** Cada tipo nuevo (S3, SharePoint) es un extractor escrito una vez, no una pieza por tabla; uno remoto descargaría el fichero a un directorio temporal que borra al terminar la propia tarea. Cuando el fichero no es CSV (por ejemplo un xlsx) hace falta además un conversor a CSV antes de la carga, con decisiones sobre hoja, cabeceras y tipos que el contrato tendría que declarar. El cargador hoy rechaza cualquier formato que no sea CSV.

**Siguiente paso.** Añadir el extractor que se necesite y poner su `type` en `source.yml`. Si hubiera varios orígenes a la vez, `source.yml` pasaría a ser una lista de orígenes.

---

<!-- Siguiente punto: copiar la plantilla de arriba -->
