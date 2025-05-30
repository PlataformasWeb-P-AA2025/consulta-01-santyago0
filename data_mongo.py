import pandas as pd
from pymongo import MongoClient

# Crear la conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["tournaments"]
collection = db["matches"]

# Cargar los datos desde un archivo en excel
# Creamos una función que recibe la ruta del archivo
def carga_datos(ruta_archivo):
    df = pd.read_excel(ruta_archivo, sheet_name=0) # Con pandas lee el archivo
    filas = df.to_dict(orient='records') # Por cada fila crea una lista de diccionarios
    return filas

# Cargar los datos de los archivos 2022 y 2023
datos_2022 = carga_datos("data/2022.xlsx")
datos_2023 = carga_datos("data/2023.xlsx")

# Insertar los registros en la colección
collection.delete_many({})  # Vaciar la colección para poder ejecutar el script varias veces
collection.insert_many(datos_2022) # Insertamos todos los registros de 2022 a la vez
collection.insert_many(datos_2023) # Insertamos todos los registros de 2023 a la vez

print("Datos insertados!")

# CONSULTAS

# Consulta 1:
# Mostrar los 5 primeros partidos del año 2022
print("\nPrimeros 5 partidos de 2022:")
for partido in collection.aggregate([
    {"$match": {"$expr": {"$eq": [{"$year":"$Date"}, 2022]}}}, {"$limit":5}
    ]):
    print(partido)

print("--------------------------------------------------")

# Consulta 2:
# Contar cuántos partidos se jugaron por superficie en 2023
print("\nCantidad de partidos por superficie:")
superficies = collection.aggregate([
    {"$group": {"_id": "$Surface", "total_partidos": {"$sum": 1}}},
    {"$sort": {"total_partidos": -1}}
    ])

for superficie in superficies:
    print(f"Superficie: {superficie['_id']}, Partidos: {superficie['total_partidos']}")

print("--------------------------------------------------")
