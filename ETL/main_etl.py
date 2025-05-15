from extract import obtener_rutas_adjuntos, leer_archivo_presupuesto, leer_archivo_ventas
from transform import limpiar_datos_ventas, limpiar_datos_presupuesto

# Paso 1: Obtener las rutas de los archivos descargados
ruta_ventas, ruta_presupuesto = obtener_rutas_adjuntos()

# Validar si se encontraron ambos archivos
if not ruta_ventas:
    print("[ERROR] No se encontró el archivo de ventas en la carpeta 'adjuntos'.")
if not ruta_presupuesto:
    print("[ERROR] No se encontró el archivo de presupuesto en la carpeta 'adjuntos'.")

# Paso 2: Leer los datos si ambos archivos están presentes
if ruta_ventas and ruta_presupuesto:
    df_ventas = leer_archivo_ventas(ruta_ventas)
    df_ppto = leer_archivo_presupuesto(ruta_presupuesto)

    # Paso 3: Aplicar transformaciones de limpieza
    df_ventas_limpio = limpiar_datos_ventas(df_ventas)
    df_ppto_limpio = limpiar_datos_presupuesto(df_ppto)

    # Mostrar algunas filas de cada DataFrame limpio para verificación
    print("\n[INFO] Primeras filas del archivo de ventas (limpio):")
    print(df_ventas_limpio.head())

    print("\n[INFO] Primeras filas del archivo de presupuesto (limpio):")
    print(df_ppto_limpio.head())

    # Mostrar estructura básica
    print("\n[INFO] Columnas en ventas (limpio):", df_ventas_limpio.columns.tolist())
    print("[INFO] Columnas en presupuesto (limpio):", df_ppto_limpio.columns.tolist())
