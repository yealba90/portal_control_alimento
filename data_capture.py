# import serial
# import sqlite3
# import time

# # Configura la comunicación serial
# ser = serial.Serial(
#     port='COM1',           # Cambia a tu puerto COM real
#     baudrate=19200,         # Configura el baud rate como en tu dispositivo
#     parity=serial.PARITY_NONE,
#     stopbits=serial.STOPBITS_ONE,
#     bytesize=serial.EIGHTBITS,
#     timeout=1               # Tiempo de espera de lectura
# )

# # Conecta o crea la base de datos SQLite
# conn = sqlite3.connect('hl10s_data.db')
# cursor = conn.cursor()

# # Crea la tabla para almacenar los datos, si no existe
# cursor.execute('''
#     CREATE TABLE IF NOT EXISTS weight_data (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         device_id INTEGER,
#         bin_a REAL,
#         bin_b REAL,
#         bin_c REAL,
#         bin_d REAL,
#         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
# conn.commit()

# # Función para enviar el comando y recibir datos
# def query_device(device_id):
#     # Construye el comando SnnPP
#     command = f'S{device_id:02}PP\r'
#     ser.write(command.encode())  # Envía el comando en ASCII

#     # Lee la respuesta
#     response = ser.readline().decode().strip()
#     if response:
#         # Procesa la respuesta si está en el formato esperado
#         bins = response.split(',')
#         if len(bins) == 4:
#             try:
#                 # Convierte a valores numéricos, manejando posibles errores
#                 bin_a = float(bins[0])
#                 bin_b = float(bins[1])
#                 bin_c = float(bins[2])
#                 bin_d = float(bins[3])

#                 # Almacena en la base de datos
#                 cursor.execute('''
#                     INSERT INTO weight_data (device_id, bin_a, bin_b, bin_c, bin_d)
#                     VALUES (?, ?, ?, ?, ?)
#                 ''', (device_id, bin_a, bin_b, bin_c, bin_d))
#                 conn.commit()
#                 print(f"Datos almacenados para el dispositivo {device_id}: {bins}")
#             except ValueError:
#                 print("Error de formato en la respuesta:", response)
#         else:
#             print("Respuesta inesperada:", response)
#     else:
#         print(f"No se recibió respuesta del dispositivo {device_id}")

# # Consulta varios IDs de dispositivos
# device_ids = [1, 2, 3, 4, 5]  # Ajusta la lista de IDs según tus dispositivos

# # Consulta los dispositivos en un ciclo
# try:
#     while True:
#         for device_id in device_ids:
#             query_device(device_id)
#             time.sleep(1)  # Espera un segundo entre consultas para evitar saturar el bus
#         time.sleep(10)  # Espera 10 segundos antes del próximo ciclo de consultas
# except KeyboardInterrupt:
#     print("Proceso terminado por el usuario.")

# import serial
# import sqlite3
# import time

# # Configura la comunicación serial
# ser = serial.Serial(
#     port='COM1',           # Cambia a tu puerto COM real
#     baudrate=19200,         # Configura el baud rate como en tu dispositivo
#     parity=serial.PARITY_NONE,
#     stopbits=serial.STOPBITS_ONE,
#     bytesize=serial.EIGHTBITS,
#     timeout=1               # Tiempo de espera de lectura
# )

# # Conecta o crea la base de datos SQLite
# conn = sqlite3.connect('hl10s_data.db')
# cursor = conn.cursor()

# # Crea la tabla para almacenar los datos, si no existe
# cursor.execute('''
#     CREATE TABLE IF NOT EXISTS weight_data (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         device_id INTEGER,
#         bin_a REAL,
#         bin_b REAL,
#         bin_c REAL,
#         bin_d REAL,
#         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
# conn.commit()

# # Función para enviar el comando y recibir datos
# def query_device(device_id):
#     # Construye el comando SnnPP
#     command = f'S{device_id:02}PP\r'
#     ser.write(command.encode())  # Envía el comando en ASCII

#     # Lee la respuesta
#     response = ser.readline().decode().strip()
#     if response:
#         # Procesa la respuesta si está en el formato esperado
#         bins = response.split(',')
#         if len(bins) == 4:
#             try:
#                 # Convierte a valores numéricos, manejando posibles errores
#                 bin_a = float(bins[0])
#                 bin_b = float(bins[1])
#                 bin_c = float(bins[2])
#                 bin_d = float(bins[3])

#                 # Almacena en la base de datos
#                 cursor.execute('''
#                     INSERT INTO weight_data (device_id, bin_a, bin_b, bin_c, bin_d)
#                     VALUES (?, ?, ?, ?, ?)
#                 ''', (device_id, bin_a, bin_b, bin_c, bin_d))
#                 conn.commit()
#                 print(f"Datos almacenados para el dispositivo {device_id}: {bins}")
#             except ValueError:
#                 print("Error de formato en la respuesta:", response)
#         else:
#             print("Respuesta inesperada:", response)
#     else:
#         print(f"No se recibió respuesta del dispositivo {device_id}")

# # Consulta varios IDs de dispositivos
# device_ids = [1, 2, 3, 4, 5]  # Ajusta la lista de IDs según tus dispositivos

# # Consulta los dispositivos en un ciclo
# try:
#     while True:
#         for device_id in device_ids:
#             query_device(device_id)
#             time.sleep(1)  # Espera un segundo entre consultas para evitar saturar el bus
#         time.sleep(10)  # Espera 10 segundos antes del próximo ciclo de consultas
# except KeyboardInterrupt:
#     print("Proceso terminado por el usuario.")

# # Cierra la conexión serial y la base de datos al finalizar
# ser.close()
# conn.close()

import os
import serial
import time
import sqlite3


# Configuración de la comunicación serial
ser = serial.Serial(
    port='COM1',           # Cambia a tu puerto COM real
    baudrate=19200,         # Configura el baud rate según tu dispositivo
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeout=1               # Tiempo de espera para la lectura
)

# Configuración de la base de datos
db_folder = "data"
db_path = os.path.join(db_folder, "hl10s_data.db")

# Crear la carpeta y la base de datos si no existen
if not os.path.exists(db_folder):
    os.makedirs(db_folder)
    print(f"Carpeta creada: {db_folder}")

# Crear y conectar la base de datos si no existe
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Crear la tabla si no existe
cursor.execute('''
    CREATE TABLE IF NOT EXISTS weight_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id INTEGER,
        bin_a REAL,
        bin_b REAL,
        bin_c REAL,
        bin_d REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')
conn.commit()

print(f"Base de datos lista en: {db_path}")

# Función para enviar el comando y recibir los datos
def query_device(device_id):
    # Construye el comando en ASCII (SnnPP)
    command = f'S{device_id:02}PP\r'
    ser.write(command.encode())  # Envía el comando en ASCII

    # Lee la respuesta
    response = ser.readline().decode().strip()  # Lee y decodifica la respuesta
    if response:
        print(f"Respuesta del dispositivo {device_id}: {response}")  # Muestra en pantalla la respuesta
        bins = response.split(',')
        if len(bins) == 4:
            try:
                # Convierte a valores numéricos
                bin_a = float(bins[0])
                bin_b = float(bins[1])
                bin_c = float(bins[2])
                bin_d = float(bins[3])

                # Almacena en la base de datos
                cursor.execute('''
                    INSERT INTO weight_data (device_id, bin_a, bin_b, bin_c, bin_d)
                    VALUES (?, ?, ?, ?, ?)
                ''', (device_id, bin_a, bin_b, bin_c, bin_d))
                conn.commit()

                print(f"Datos guardados en la DB para el dispositivo {device_id}: {bins}")
            except ValueError:
                print("Error de formato en la respuesta:", response)
        else:
            print("Respuesta inesperada:", response)
    else:
        print(f"No se recibió respuesta del dispositivo {device_id}")

# Lista de IDs de los dispositivos a consultar
device_ids = [1, 2, 3, 4, 5]  # Ajusta la lista de IDs según los dispositivos conectados

# Consulta los dispositivos en un ciclo
try:
    while True:
        for device_id in device_ids:
            # Enviar dos veces por ID
            for _ in range(2):
                query_device(device_id)
                time.sleep(1)  # Espera 1 segundo entre las dos consultas al mismo ID
            time.sleep(1)  # Espera 1 segundo antes de pasar al siguiente ID
        time.sleep(10)  # Espera 10 segundos antes del próximo ciclo completo
except KeyboardInterrupt:
    print("Proceso terminado por el usuario.")
finally:
    # Cierra la conexión serial y la base de datos al finalizar
    ser.close()
    conn.close()
    print("Conexión serial y base de datos cerradas.")