import os

def crear_prueba():
    # Crear carpeta
    if not os.path.exists("imagenes/Villarrica"):
        os.makedirs("imagenes/Villarrica")
    
    # Crear un archivo de texto de prueba
    with open("imagenes/Villarrica/prueba_conexion.txt", "w") as f:
        f.write("Si puedes leer esto, el bot tiene permisos de escritura correctamente.")
    
    # Crear el CSV de prueba
    with open("registro_vrp.csv", "w") as f:
        f.write("Volcan,Estado\nVillarrica,Conectado")
    
    print("Archivos de prueba creados exitosamente.")

if __name__ == "__main__":
    crear_prueba()
