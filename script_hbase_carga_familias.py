import happybase
import pandas as pd

# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")

    # 2. Crear la tabla con las familias de columnas
    table_name = 'familias_en_accion'
    families = {
        'personal': dict(),     # Datos personales y demográficos
        'ubicacion': dict(),    # Localización geográfica
        'beneficios': dict(),   # Beneficios asignados y relacionados
        'estado': dict()        # Estado general en el programa
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'familias_en_accion' creada exitosamente")

    # 3. Cargar datos del CSV
    df = pd.read_csv('rows.csv')

    # Iterar sobre el DataFrame usando el índice
    for index, row in df.iterrows():
        row_key = f'familia_{index}'.encode()

        data = {
            b'personal:genero': str(row['Genero']).encode(),
            b'personal:edad': str(row['RangoEdad']).encode(),
            b'personal:discapacidad': str(row['Discapacidad']).encode(),
            b'personal:etnia': str(row['Etnia']).encode(),
            b'personal:escolaridad': str(row['NivelEscolaridad']).encode(),
            b'personal:tipo_documento': str(row['TipoDocumento']).encode(),
            b'personal:tipo_poblacion': str(row['TipoPoblacion']).encode(),

            b'ubicacion:departamento': str(row['NombreDepartamentoAtencion']).encode(),
            b'ubicacion:municipio': str(row['NombreMunicipioAtencion']).encode(),

            b'beneficios:tipo': str(row['TipoBeneficio']).encode(),
            b'beneficios:asignacion': str(row['TipoAsignacionBeneficio']).encode(),
            b'beneficios:rango_consolidado': str(row['RangoBeneficioConsolidadoAsignado']).encode(),
            b'beneficios:rango_ultimo': str(row['RangoUltimoBeneficioAsignado']).encode(),
            b'beneficios:fecha_ultimo': str(row['FechaUltimoBeneficioAsignado']).encode(),
            b'beneficios:inscripcion': str(row['FechaInscripcionBeneficiario']).encode(),
            b'beneficios:cantidad': str(row['CantidadDeBeneficiarios']).encode(),

            b'estado:bancarizado': str(row['Bancarizado']).encode(),
            b'estado:estado_beneficiario': str(row['EstadoBeneficiario']).encode(),
            b'estado:titular': str(row['Titular']).encode()
        }

        table.put(row_key, data)

    print("Datos cargados exitosamente en HBase")

    # 4. Análisis simple: contar beneficiarios por municipio
    print("\n=== Conteo de beneficiarios por municipio ===")
    municipio_stats = {}
    for index, row in df.iterrows():
        municipio = row['NombreMunicipioAtencion']
        municipio_stats[municipio] = municipio_stats.get(municipio, 0) + 1

    for municipio, total in municipio_stats.items():
        print(f"{municipio}: {total} beneficiarios")

    # 5. Análisis por tipo de beneficio
    print("\n=== Conteo por tipo de beneficio ===")
    tipo_beneficio_stats = {}
    for index, row in df.iterrows():
        tipo = row['TipoBeneficio']
        tipo_beneficio_stats[tipo] = tipo_beneficio_stats.get(tipo, 0) + 1

    for tipo, total in tipo_beneficio_stats.items():
        print(f"{tipo}: {total} registros")

    # 6. Promedio de beneficiarios por familia
    print("\n=== Promedio de beneficiarios por familia ===")
    promedio = df['CantidadDeBeneficiarios'].mean()
    print(f"Promedio: {promedio:.2f}")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    connection.close()
