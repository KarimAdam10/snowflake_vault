# Uso de la IA en este trabajo

Todo el trabajo se ha realizado con **Claude** (Anthropic), y con ninguna otra herramienta de IA. Se desarrolló en una conversación larga, de en torno a ochenta mensajes, así que este documento no lista todos los prompts: recoge los **representativos de la forma de trabajar**, literales y con sus erratas, y explica cómo se usó la IA.

## 1. Cómo se trabajó

- **Primero el concepto y el plan, después el código.** Pedí el mapa conceptual y la justificación antes de implementar, y para las decisiones de diseño pedí respuestas sin implementar nada. Los cambios grandes se hicieron con un plan por escrito que yo aprobaba o recortaba antes de ejecutar.
- **Pasos pequeños y verificables.** Prueba mínima primero (un hub y un satélite con los dos lotes) y después el resto, nunca todo de golpe.
- **Cuestionar hasta entender y dejarlo documentado.** No di por buena ninguna capa o decisión que no entendiera; la IA tenía que justificarla y las decisiones importantes se escribieron en `docs/`.
- **Verificar con datos, no con la palabra de la IA.** Los resultados se contrastaron con un cálculo independiente desde los CSV, con ejecuciones reales de los dos lotes y con las pruebas de dbt. Los permisos de Snowflake se probaron intentando operaciones prohibidas y viendo el error.
- **Control de los cambios.** Todo se revisó antes de incorporarlo, pasó por ramas y pull requests, y deshice cambios de la IA cuando no me convencían.

Reglas que fui fijando: respuestas breves y sin hacer nada que no pidiera, ir de lo sencillo a lo complejo, nada que sobrara respecto al enunciado, y nada fuera del repositorio.

## 2. Prompts representativos

- «Quiero que planees primero como modelarías los datos y como quedaría (ideal/conceptualmente) el VAULT… dame primero el mapa conceptual y luego como lo llevarías a cabo»
- «Dos cosas de caras a futuro. No implementas nada y solo responde a las preguntas. Como realizarías un CI/CD de este repositorio?»
- «El plan era muy ambicioso porque solucionaba ya todo el proyecto. Quiero que realices las siguientes acciones más concretas para ir empezando»
- «haz la prueba mínima: hub_customer y sat_customer con los dos lotes… Ya despues estudiaré todo y sacaré conclusiones»
- «Yo sigo sin entender la capa de STAGING, por lo que veo ni siquiera es una capa como tal fisica… esto se podría hacer en la metadata»
- «y porque no se calculan los hashes cuando se pasa de landing al vault?»
- «Tengo muchas preguntas de lo realizado, pero quiero que las respondas brevemente. Ya posteriormente si lo necesito te indicaré de profundizar en cada cuestión necesaria»
- «Necesito del punto 1 y 3 profundizar más en como por ejemplo se ejecuta la unica macro que tenemos para decidir el esquema. Y luego, saber si es posible crear una macro que cree el HASHDIFF para poder quitarnos la capa staging?»
- «sobre el modelo del raw vault, no podrían nation i/o region ser references tables?»
- «Necesito crear documentos en la carpeta de docs con las decisiones técnicas más importantes… la justificación de añadir la capa de staging y la explicación de una alternativa sin esa capa»
- «Vale, tres preguntas. … en nuestro data mart o modelo dimensional no faltan dims (como la dim_order)? Si es así no las añadas aun ni nada, solo responde»
- «mira todos los requerimientos del enunciado del ejercicio, haz un listado y di cuales estan satisfechos, cuales no y cuales parcialmente»
- «No veo claro el modelo. Olvidate de lo que se ha hecho hasta ahora… dame escrita una respuesta con el modelo que tu generarías para una situación de negocio que realmente requiera de ese modelo»
- «Sí, hazlo: elimina los metadatos de mart y haz que queden solo los modelos, también hazlo con los metadatos del business_vault»
- «Sobre el CI/CD, piensa que no tengo acceso a Azure pipelines ni nada por el estilo… solo puedo trabajarlo como mucho en mi GitHub personal y con Docker… quiero que me digas un par de propuestas a nivel tecnico (hazlo breve)»
- «Veo muchísima complejidad en lo implementado en la ultima iteración… No quiero la implementación de una lógica para que Prod sea solo lectura»
- «No podemos asumir que estan bien o ignorarlos en el git y como esta orquestada la generación por parte del airflow pues ya asumir que ya en la ejecución se ejecutará»
- «Lo que hay en la carpeta tests es necesario? No quiero cosas innecesarias o que son un además de lo que se pide en el enunciado»
- «porque el dbt-parse y docker en el ci? Cual es la funcion de esto?»
- «nada haz rollback de estos ultimos cambios, mea culpa, era mejor dejarlo simplificado como antes»
- «Vamos a comprobar ahora la ejecución desde ambos entornos. Trata hacerlo tu de dev y luego dame el manual a mi»
- «Actualiza el cuadro con los cumplimientos y reenvialo con el estado actual del repo, no hagas nada más»
- «Voy a darte el plan de acción que vamos a seguir para acabar este proyecto… Crees que me falta algo a considerar?»
- «Quiero que ayudes mucho teoricamente con este punto. Lo primero que quiero es una breve introducción a Snowflake…»
- «Si he entendido bien aquí el warehouse no es el típico tipo de almacenamiento de datos de las arquitecturas de data sino el ejecutador de las consultas?»
- «Dame el plan de lo que implementarías ahora mismo… posteriormente ejecutarás el plan y por ultimo cuestionaremos juntos alternativas y posibles cambios»
- «Manual para prod realizalo tu porfa y comprueba que se ha hecho todo bie y podemos dar por cerrado el tema snowflake y mart»
- «Quiero también que me des el modelo CORRECTO, DECENTE Y QUE CUMPLA más simple que puedas generar con estos datasets, raw vault y business vault»
- «La conclusión que quiero sacar es si sería justificable un modelo como el que tenemos pero sin fact_order»
- «quiero podar las malas hierbas, necesito saber archivos que se pueden simplificar o directamente eliminar sin perder requisitos del enunciado»

## 3. Dónde corregí o descarté lo que propuso la IA

La IA propuso mucho y a veces se equivocó o fue más allá de lo necesario:

- **Sobreingeniería, que retiré.** Los hechos del mart totalmente por metadatos, un despliegue automático a producción con lógica de solo lectura y una suite de tests propia: la complejidad no compensaba y pedí quitarlos.
- **Planes demasiado ambiciosos.** El primer plan resolvía todo el proyecto de golpe; lo recorté a pasos concretos.
- **Decisiones de diseño mías.** Que la capa LANDING no acumule lotes (cada carga sustituye a la anterior) fue decisión mía contra una propuesta distinta, y la capa de staging solo se mantuvo cuando su justificación me convenció.
- **Errores de la IA que se detectaron.** Afirmó que AutomateDV no soportaba tablas de referencia, y era falso (se comprobó leyendo el paquete). Una prueba de permisos dio un falso positivo por los roles secundarios del usuario, y se detectó y repitió. Un control del propio script impidió borrar una base de datos que contenía datos, contra lo que la IA suponía.

## 4. Limitaciones

El objetivo era que la IA acelerase procesos y fuera un soporte, pero **A PESAR DE ESO SER CAPAZ DE DAR RESPUESTA Y SER EL PRINCIPAL RESPONSABLE DE LO IMPLEMENTADO Y REALIZADO**.

La IA puede equivocarse con seguridad aparente, por lo que cada resultado importante se contrastó con datos, con ejecuciones o con Snowflake, y el código generado se revisa y se estudia como parte del trabajo. La conversación completa se conserva por si se quiere contrastar.
