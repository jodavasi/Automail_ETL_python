# ETL Automatizado con Python, BigQuery y Power BI

#Nota
Los dataset utilizados fueron generados aleatoriamente y no forman parte de material sensible de ninguna empresa.

Este proyecto implementa una solución de ETL automatizada que permite recuperar archivos Excel desde una cuenta de correo electrónico, procesarlos con Python, almacenarlos en Google BigQuery y usarlos como fuente de datos para dashboards en Power BI.

## 🔧 Tecnologías utilizadas

- **Python 3.10+**
- **IMAP / Email**
- **Pandas**
- **Google Cloud BigQuery**
- **Power BI**
- **crontab / tareas programadas**

<img width="200" height="200" alt="image" src="https://github.com/user-attachments/assets/b1c6b027-598d-4363-ad55-a4d44cf588c0" />
<img width="353" height="200" alt="image" src="https://github.com/user-attachments/assets/0059b334-c634-4247-be20-702e6c9e0612" />
<img width="200" height="200" alt="image" src="https://github.com/user-attachments/assets/2d7eb7a6-8a1e-4046-9a38-ee6d0e61c05f" />
<img width="353" height="200" alt="image" src="https://github.com/user-attachments/assets/3fc41ce8-c57c-4a39-903a-4b2328d34019" />
<img width="299" height="168" alt="image" src="https://github.com/user-attachments/assets/7c846f7c-f355-495f-9735-ec8c5b14d0d8" />


## Caso de uso

El objetivo del proyecto es automatizar el procesamiento de reportes financieros enviados periódicamente por correo electrónico. Este tipo de solución se aplicó a un escenario real en Grupo Pozuelo, donde era necesario consolidar archivos Excel enviados regularmente por proveedores o departamentos internos, transformarlos y analizarlos en Power BI.

## ¿Qué hace este proyecto?

1. **Conexión al correo electrónico**  
   El script accede a una bandeja de entrada (por IMAP), busca correos con archivos Excel adjuntos según criterios definidos (fecha, asunto, remitente), y descarga los archivos.

2. **Procesamiento de datos con pandas**  
   Los archivos se leen con `pandas`, se transforman y limpian según las necesidades analíticas (normalización de columnas, tipos de datos, nombres estándar, columnas calculadas).

3. **Carga a BigQuery**  
   Los datos transformados se cargan en una tabla en BigQuery, permitiendo su uso en herramientas de BI como Power BI o Looker Studio.

4. **Visualización en Power BI**  
   Power BI se conecta directamente a BigQuery para consumir la información actualizada y mostrarla en dashboards interactivos.


## Seguridad

- Las credenciales de acceso al correo y a Google Cloud están gestionadas en archivos `.env` o JSON (y excluidas del repositorio con `.gitignore`).
- Se recomienda usar un servicio de gestión de secretos o variables de entorno en producción.

## Automatización

El flujo puede ser programado para ejecutarse automáticamente mediante `cron` en sistemas Linux o `Task Scheduler` en Windows. También es posible implementarlo como una función en Google Cloud Functions o Cloud Run para ejecución serverless.

## Resultados

- Ahorro de tiempo en tareas manuales de recopilación de datos.
- Datos actualizados automáticamente cada vez que llega un nuevo correo.
- Dashboards conectados a una única fuente confiable en BigQuery.

## Futuras mejoras

- Integración con Google Drive como fuente alternativa.
- Validación de calidad de datos antes de cargar a BigQuery.
- Registro de logs de ejecución.
- Notificaciones por correo o Slack al finalizar el proceso.

---

**Autor:** José Daniel Vargas Sibaja  
[lineadan@gmail.com]
[(https://www.linkedin.com/in/jodavasi/)]






