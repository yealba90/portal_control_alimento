import os
import sqlite3
import csv
import time
import serial

# Función para configurar la conexión serial
def setup_serial_connection(port='COM3', baudrate=9600):
    try:
        ser = serial.Serial(
            port=port,
            baudrate=baudrate,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS,
            timeout=1
        )
        print(f"Conexión serial establecida en {port} con baudrate {baudrate}")
        return ser
    except Exception as e:
        print(f"Error al configurar la conexión serial: {e}")
        return None

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

# Función para realizar consultas reales a un dispositivo
def query_device(ser, cursor, conn, device):
    id_silo = int(device['ID_SILO'])
    id_db_columns = [f'ID_DB_{i}' for i in range(1, 5)]
    id_dbs = [int(device[col]) for col in id_db_columns if device[col]]

    # Construir el comando en ASCII
    command = f'S{id_silo:02}PP\r'
    ser.write(command.encode())  # Enviar el comando al dispositivo
    time.sleep(1)  # Esperar una breve pausa antes de leer la respuesta

    # Leer la respuesta
    response = ser.readline().decode().strip()
    if response:
        print(f"Respuesta del dispositivo {id_silo}: {response}")
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
    else:
        print(f"No se recibió respuesta del dispositivo {id_silo}")

# Función principal
def main():
    # Configurar la conexión serial
    ser = setup_serial_connection(port='COM3', baudrate=9600)
    if not ser:
        return

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
                    query_device(ser, cursor, conn, device)
                    last_query_times[id_silo] = current_time  # Actualizar el tiempo de la última consulta

            time.sleep(1)  # Pequeña pausa para evitar consumir demasiados recursos
    except KeyboardInterrupt:
        print("Proceso terminado por el usuario.")
    finally:
        # Cierra la conexión serial y la base de datos al finalizar
        ser.close()
        conn.close()
        print("Conexión serial y base de datos cerradas.")

# Ejecutar el programa principal
if __name__ == "__main__":
    main()
