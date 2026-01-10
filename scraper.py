import requests

# URL de prueba (Villarrica MODIS)
url = "https://www.mirovaweb.it/NRT/volcanoDetails_MOD.php?volcano_id=357120"

print(f"--- ANALIZANDO CÓDIGO FUENTE DE: {url} ---")

try:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers, timeout=15)
    
    if response.status_code == 200:
        texto = response.text
        lines = texto.splitlines()
        
        found = False
        print("\n🔍 BUSCANDO 'Last Update' o fechas en el código fuente:\n")
        
        # Imprimir las primeras 500 líneas por si acaso está al principio
        # y buscar específicamente patrones de fecha
        for i, line in enumerate(lines):
            # Buscamos la etiqueta exacta o el año 2025/2026
            if "Last Update" in line or "2025" in line or "2026" in line:
                print(f"Línea {i}: {line.strip()}")
                found = True
        
        if not found:
            print("❌ No encontré la frase exacta. Imprimiendo parte del HTML para revisión manual:")
            # Imprimir un trozo del HTML para ver qué está pasando
            print(texto[:2000]) 
        else:
            print("\n✅ ¡Pistas encontradas! Copia las líneas de arriba.")
            
    else:
        print(f"Error al cargar página: {response.status_code}")

except Exception as e:
    print(f"Error crítico: {e}")
