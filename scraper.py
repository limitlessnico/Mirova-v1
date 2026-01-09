import requests
import os

def diagnostico():
    print("--- INICIANDO DIAGNÓSTICO ---")
    url = "https://www.mirovaweb.it/NRT/volcanoDetails_MOD.php?volcano_id=357120"
    
    try:
        print(f"Intentando conectar a: {url}")
        res = requests.get(url, timeout=20)
        print(f"Código de respuesta: {res.status_code}")
        
        if res.status_code == 200:
            print("Conexión exitosa. El sitio está respondiendo.")
            # Crear un archivo pequeño para forzar el commit
            if not os.path.exists("imagenes"): os.makedirs("imagenes")
            with open("imagenes/log_diagnostico.txt", "w") as f:
                f.write(f"Conexión exitosa a las {res.headers.get('Date')}")
        else:
            print(f"Error: El sitio devolvió un código {res.status_code}")
            
    except Exception as e:
        print(f"ERROR CRÍTICO DE CONEXIÓN: {e}")

if __name__ == "__main__":
    diagnostico()


