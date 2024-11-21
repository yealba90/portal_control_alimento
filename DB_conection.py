import pyodbc
import configparser
import os
from dotenv import load_dotenv

class InsertData():
    #"""Clase para insertar datos en BD"""

    def __init__(self):
        # Rutas de archivos de configuraci�n
        dotenv_path = os.path.join(os.getcwd(), '.env')
        
        load_dotenv(dotenv_path)

        bd_historical_divices = {
            "HISTORICAL_DEVICES_DLLO": {
                "driver": os.getenv("BD_HISTORICAL_DEVICES_DRIVER"),
                "server": os.getenv("BD_HISTORICAL_DEVICES_SERVER_DLLO"),
                "database": os.getenv("BD_HISTORICAL_DEVICES_DLLO"),
                "uid": os.getenv("UID_BD_HISTORICAL_DEVICES"),
                "pwd": os.getenv("PWD_HISTORICAL_DEVICES_DLLO")
            },
            "HISTORICAL_DEVICES_PROD": {
                "driver": os.getenv("BD_FACTURAS_1N_DRIVER"),
                "server": os.getenv("BD_FACTURAS_1N_SERVER_PROD"),
                "database": os.getenv("BD_FACTURAS_1N_PROD"),
                "uid": os.getenv("UID_BD_FACTURAS_1N"),
                "pwd": os.getenv("PWD_FACTURAS_1N_PROD")
            }
        }

        self.conexion_info = bd_historical_divices["HISTORICAL_DEVICES_DLLO"]

    def __get_database_connection(self):
        #"""Establece una conexiOn con la base de datos."""
        return pyodbc.connect(
            f"Driver={{{self.conexion_info['driver']}}};"
            f"Server={self.conexion_info['server']};"
            f"Database={self.conexion_info['database']};"
            f"UID={self.conexion_info['uid']};"
            f"PWD={self.conexion_info['pwd']};")
    
    def consultar_db(self):
        print(f'******************************')

        # Verifica si una factura en la base de datos
        connection = self.__get_database_connection()
        cursor = connection.cursor()      
    
        # Ejecuta la consulta usando parámetros
        query = """
            SELECT * 
            FROM [control_de_alimentos].HistoricalDevices;
        """
        
        cursor.execute(query)
        result = cursor.fetchall()

        # Imprimir los resultados
        for row in result:
            print(row)

        cursor.close()
        connection.close()

        return result if result else None

# Crear instancia y llamar al método
prueba = InsertData()
prueba.consultar_db()