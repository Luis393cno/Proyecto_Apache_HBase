
# Proyecto: Carga y análisis de datos en Apache HBase – Familias en Acción

## Descripción

Este proyecto tiene como objetivo la carga, almacenamiento y análisis de información del programa **Familias en Acción** en una base de datos **NoSQL HBase**. Utiliza Python con la biblioteca `happybase` para conectarse a HBase y cargar los datos desde un archivo CSV oficial.

Se implementa un esquema basado en **column families** que facilita la organización de los datos en categorías relevantes: `personal`, `ubicacion`, `beneficios` y `estado`.

## Requisitos

- Python 3
- Apache HBase en ejecución local (con Thrift activado)
- Paquetes de Python: `happybase`, `pandas`

Puedes instalarlos ejecutando:

```
pip install happybase pandas
```

## Dataset

El conjunto de datos se obtiene directamente del portal de datos abiertos del Gobierno de Colombia. Para descargar el archivo CSV, utiliza el siguiente comando en la terminal:

```
wget https://www.datos.gov.co/api/views/xswh-ityu/rows.csv
```

Este archivo contiene información detallada sobre los beneficiarios del programa: ubicación, tipo de beneficio, rango, escolaridad, entre otros datos clave.

## Estructura de la tabla HBase

- **Nombre de la tabla:** `familias_en_accion`
- **Families de columnas:**
  - `personal`: Información personal y demográfica
  - `ubicacion`: Departamento y municipio de atención
  - `beneficios`: Tipo y fecha del beneficio asignado
  - `estado`: Bancarización, titularidad y estado general

## Ejecución del script

Una vez descargado el dataset, ejecuta el script Python para cargar los datos en HBase:

```
python cargar_hbase.py
```

El script:
1. Conecta a HBase.
2. Elimina la tabla si ya existe.
3. Crea la tabla con las familias de columnas mencionadas.
4. Carga y transforma los datos desde el CSV.
5. Inserta cada fila en la tabla con una clave única (`familia_{index}`).

## Salida esperada

```
Conexión establecida con HBase
Eliminando tabla existente - familias_en_accion
Tabla 'familias_en_accion' creada exitosamente
Datos cargados exitosamente en HBase
```
