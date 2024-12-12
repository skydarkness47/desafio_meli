import csv
import json
import sqlite3
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os


# Crear las tablas necesarias en la base de datos
def create_tables(cursor):
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS bases (
            id_db INTEGER PRIMARY KEY AUTOINCREMENT,
            db_name TEXT,
            clasificacion TEXT
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS users (
            id_user INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT,
            estado TEXT,
            rol TEXT
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS asignaciones (
            db_id INTEGER,
            owner_id INTEGER,
            manager_id INTEGER,
            FOREIGN KEY (db_id) REFERENCES bases (id_db),
            FOREIGN KEY (owner_id) REFERENCES users (id_user),
            FOREIGN KEY (manager_id) REFERENCES users (id_user)
        )"""
    )

# Procesar el archivo CSV que contiene los usuarios
def process_csv(user_csv):
    csv_data = []
    for line in csv.reader(user_csv.strip().split('\n')):  # Leer cada línea del CSV
        csv_data.append(line)
    return csv_data[1:]  # Excluir la primera línea (headers)

# Insertar usuarios y managers en la base de datos
def insert_users_and_owners(cursor, user_rows):
    user_ids = {}
    for row in user_rows:
        email, estado, manager_email = row[1], row[2], row[3]  # Extraer datos del usuario
        cursor.execute(
            """
            INSERT INTO users (email, estado, rol)
            VALUES (?, ?, ?)
            """,
            (email, 'activo', 'owner')  # Insertar usuario como "owner"
        )
        owner_id = cursor.lastrowid  # Obtener el ID del usuario insertado

        if manager_email not in user_ids:  # Verificar si el manager ya existe
            cursor.execute(
                """
                INSERT INTO users (email, estado, rol)
                VALUES (?, ?, ?)
                """,
                (manager_email, 'activo', 'manager')
            )
            user_ids[manager_email] = cursor.lastrowid  # Guardar el ID del manager

    return user_ids

# Insertar las bases de datos en la tabla correspondiente
def insert_bases(cursor, data):
    for row in data:
        db, clas = row['db_name'], row["clasificacion"]  # Extraer nombre y clasificación de la base
        cursor.execute(
            """
            INSERT INTO bases (db_name, clasificacion)
            VALUES (?, ?)
            """,
            (db, clas)  # Insertar los datos
        )

# Asignar las bases a los dueños y managers correspondientes
def assign_bases(cursor, data, user_rows, user_ids):
    cursor.execute("SELECT id_user, email FROM users")
    user3 = cursor.fetchall()
    cursor.execute("SELECT id_db, db_name FROM bases")
    bases = cursor.fetchall()

    for record in data:
        db_name = record.get('db_name')  # Obtener el nombre de la base
        owner_email = record.get('owner_email')  # Obtener el email del dueño
        db_id = next((base[0] for base in bases if base[1] == db_name), None)  # Buscar ID de la base
        ownerid = next((user[0] for user in user3 if user[1] == owner_email), None)  # ID del dueño
        managermail = next((manager[3] for manager in user_rows if manager[1] == owner_email), None)  # Email del manager
        managerid = next((manager[0] for manager in user3 if manager[1] == managermail), None)  # ID del manager

        cursor.execute(
            """
            INSERT INTO asignaciones (db_id, owner_id, manager_id)
            VALUES (?, ?, ?)
            """,
            (db_id, ownerid, managerid)  # Insertar la asignación
        )
def delete_if_exists():

    # Ruta del archivo
    file_path = "revalida_clasificacion.db"

    # Verificar si el archivo existe
    if os.path.exists(file_path):
        # Si existe, borrarlo
        os.remove(file_path)
        print(f"Archivo '{file_path}' eliminado.")
    else:
        print(f"El archivo '{file_path}' no existe.")

# Preparar los datos para enviar correos electrónicos
def prepare_mail(cursor):
    cursor.execute("SELECT id_user, email, rol FROM users")
    user3 = cursor.fetchall()
    cursor.execute("SELECT id_db, db_name, clasificacion FROM bases WHERE clasificacion = 'high'")
    bases = cursor.fetchall()
    cursor.execute("SELECT db_id, owner_id, manager_id FROM asignaciones")
    asignaciones = cursor.fetchall()

    for asig in asignaciones:
        id_manager = next((asig[2] for base in bases if base[0] == asig[0]), None)  # ID del manager
        mail_manager = next((user[1] for user in user3 if user[0] == id_manager), None)  # Email del manager
        db_id = next((asig[0] for base in bases if base[0] == asig[0]), None)
        db_name = next((base[1] for base in bases if base[0] == db_id), None)

        if db_name is not None:
            send_email(mail_manager, db_name)  # Enviar email

# Función para enviar correos electrónicos
def send_email(mail, base):
    pw_google = os.getenv("GOOGLE")
    try:
        # Crear el mensaje
        msg = MIMEMultipart()
        msg['From'] = "gerencia_ciber@ml.com"
        msg['To'] = mail
        msg['Subject'] = f"Revalide y de el ok de la siguiente base {base}"
        body = "Hola, este es un correo enviado desde Python."
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login("skydarkness123@gmail.com", f"{pw_google}")

        # Enviar el correo
        server.sendmail("skydarkness123@gmail.com", mail, msg.as_string())
        server.quit()

        print(f"Correo enviado exitosamente a {mail}")

    except Exception as e:
        print(f"Error al enviar correo: {e}")

# Función principal para ejecutar el programa
def main():

    delete_if_exists()
    
    data_json = '''[
        {"db_name": "clientes_db", "owner_email": "pepe@ml.com", "clasificacion": "high"},
        {"db_name": "ventas_db", "owner_email": "franco@ml.com", "clasificacion": "medium"},
        {"db_name": "inventario_db", "owner_email": "juan@ml.com", "clasificacion": "high"}
    ]'''

    user_csv = '''id,user_id,user_estado,user_manager
1,pepe@ml.com,activo,manager1@ml.com
2,franco@ml.com,activo,manager2@ml.com
3,juan@ml.com,activo,manager3@ml.com
'''

    conn = sqlite3.connect('revalida_clasificacion.db')  # Conectar a la base de datos
    cursor = conn.cursor()
    create_tables(cursor)  # Crear tablas

    data = json.loads(data_json)  # Cargar datos JSON
    user_rows = process_csv(user_csv)  # Procesar CSV

    user_ids = insert_users_and_owners(cursor, user_rows)  # Insertar usuarios y managers
    insert_bases(cursor, data)  # Insertar bases
    assign_bases(cursor, data, user_rows, user_ids)  # Asignar bases
    prepare_mail(cursor)  # Preparar y enviar correos

    conn.commit()  # Guardar cambios
    conn.close()  # Cerrar conexión

if __name__ == "__main__":
    main()