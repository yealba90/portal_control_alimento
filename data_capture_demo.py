import os
import sqlite3
import csv
import time
import random

# Función para configurar la base de datos
def setup_database():
    db_folder = "data"
    db_path = os.path.join(db_folder, "hl10s_data.db")
    
    # Crear la carpeta si no existe
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
        print(f"Carpeta creada: {db_folder}")
    
    # Conectar a la base de datos
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Crear la tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weight_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_silo INTEGER,
            id_db INTEGER,
            weight REAL,
            send INTEGER DEFAULT 0,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    print(f"Base de datos lista en: {db_path}")
    return conn, cursor

# Función para leer devices.csv
def load_devices(file_path='devices.csv'):
    devices = []
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            devices.append(row)
    return devices

# Generador de datos aleatorios que simula la respuesta serial
def generate_serial_data():
    return f"{random.uniform(-5000, 5000):+06.0f},{random.uniform(-5000, 5000):+06.0f},{random.uniform(-5000, 5000):+06.0f},{random.uniform(-5000, 5000):+06.0f}"

# Función para realizar una consulta simulada a un dispositivo
def query_device_simulated(cursor, conn, device):
    id_silo = int(device['ID_SILO'])
    id_db_columns = [f'ID_DB_{i}' for i in range(1, 5)]
    id_dbs = [int(device[col]) for col in id_db_columns if device[col]]

    # Generar datos aleatorios simulados
    response = generate_serial_data()
    print(f"Respuesta simulada del dispositivo {id_silo}: {response}")
    
    bins = response.split(',')
    if len(bins) == 4:
        try:
            # Guardar en la base de datos
            for idx, id_db in enumerate(id_dbs):
                weight = float(bins[idx]) if idx < len(bins) else None
                if weight is not None:
                    cursor.execute('''
                        INSERT INTO weight_data (id_silo, id_db, weight, send)
                        VALUES (?, ?, ?, 0)
                    ''', (id_silo, id_db, weight))
            conn.commit()
            print(f"Datos guardados para el dispositivo {id_silo}")
        except ValueError:
            print("Error de formato en la respuesta:", response)
    else:
        print("Respuesta inesperada:", response)

# Función principal
def main():
    # Configurar la base de datos
    conn, cursor = setup_database()

    # Cargar los dispositivos desde el archivo CSV
    devices = load_devices()

    # Inicializar tiempos de última consulta para cada dispositivo
    last_query_times = {int(device['ID_SILO']): 0 for device in devices}

    try:
        while True:  # Ciclo infinito
            current_time = time.time()  # Tiempo actual en segundos
            for device in devices:
                id_silo = int(device['ID_SILO'])
                frequency = int(device['FREQUENCY']) * 60  # Convertir minutos a segundos

                # Verificar si es tiempo de consultar este dispositivo
                if current_time - last_query_times[id_silo] >= frequency:
                    query_device_simulated(cursor, conn, device)
                    last_query_times[id_silo] = current_time  # Actualizar el tiempo de la última consulta

            time.sleep(1)  # Pequeña pausa para evitar consumir demasiados recursos
    except KeyboardInterrupt:
        print("Proceso terminado por el usuario.")
    finally:
        # Cierra la base de datos al finalizar
        conn.close()
        print("Base de datos cerrada.")

# Ejecutar el programa principal
if __name__ == "__main__":
    main()