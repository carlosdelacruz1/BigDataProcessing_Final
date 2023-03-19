#### SE ME ACABO EL CREDITO EN GOOGLE CLOUD CON LO CUAL LAS CONEXIONES NO ESTAN BIEN, LAS IPs LAS HE SUSTITUIDO POR LAS TEORICAS QUE PUSE EN SU DIA

# BigDataProcessing

En resumen, este proyecto final de Big Data Processing se enfoca en la construcción de una arquitectura lambda para procesar grandes volúmenes de datos recolectados desde antenas de telefonía móvil utilizando diferentes tecnologías y herramientas para satisfacer las necesidades específicas de cada capa de la arquitectura. Además, se utilizan dos fuentes de datos diferentes que se procesan y se enriquecen para obtener resultados precisos y útiles que se almacenan en diferentes tablas de la base de datos.

En este proyecto final de procesamiento de grandes volúmenes de datos, vamos a construir una arquitectura lambda que nos permita procesar los datos recolectados desde antenas de telefonía móvil. Esta arquitectura se divide en tres capas que son la capa de procesamiento en streaming, la capa de procesamiento por lotes y la capa encargada de servir los datos.

Para cada capa de la arquitectura lambda, utilizaremos diferentes tecnologías que nos permitan satisfacer los requisitos específicos de cada una. Para la capa de procesamiento en streaming, utilizaremos Spark Structured Streaming, Apache Kafka y Google Compute Engine. Para la capa de procesamiento por lotes, utilizaremos Spark SQL y Google Cloud Storage. Y para la capa encargada de servir los datos, utilizaremos Google SQL (PostgreSQL) y Apache Superst

Trabajaremos con dos fuentes de datos diferentes. La primera fuente de datos es el uso de datos de los dispositivos móviles, que será enviada desde las antenas y llegará al sistema de mensajes de Apache Kafka en tiempo real. La segunda fuente de datos es una base de datos con información de los usuarios, que suele ser modificada por operarios a través de un servidor web.

Para procesar estas fuentes primero hay que ir al script de SQL de google cloud computing y para ello empezar por el **JBDCProvisioner** Para poder procesar estas fuentes de datos, primero debemos iniciar la instancia SQL de GCP y ejecutar el archivo "jdbcProvisioner.scala" este creara las tablas 
de metadata "user_metadata". Luego, encenderemos la máquina virtual donde se ejecutará Kafka, que generará un stream de datos que serán leídos por Scala.

El programa de Scala se suscribirá al topic "devices" de Kafka para poder consumir esos datos. Los datos leídos serán parseados a formato Json y guardados en formato parquet para que puedan ser leídos por el proceso Batch. Los datos en formato Json serán enriquecidos con metadata obtenida de la tabla "user_metadata" de la base de datos.

Posteriormente, con esos datos ya enriquecidos, se realizarán las agregaciones y los resultados se guardarán en las tablas "bytes_agg_antenna", "bytes_agg_user" y "bytes_agg_app" de la base de datos. Por otro lado, el proceso Batch leerá los archivos parquet para una hora en particular y realizará las agregaciones correspondientes, cuyos resultados se guardarán en las tablas "bytes_agg_antenna_1h", "bytes_agg_user_1h", "bytes_agg_app_1h" y "users_over_quota_1h" de la base de datos.
