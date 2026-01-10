# --- BLOQUE DE GUARDADO RESILIENTE ---
    if registros_ciclo:
        df_nuevo = pd.DataFrame(registros_ciclo)
        
        # Intentamos una maniobra de recuperación
        try:
            # Solo intentamos leer si el archivo existe Y tiene contenido real
            if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 10:
                df_base = pd.read_csv(DB_FILE)
                df_final = pd.concat([df_base, df_nuevo], ignore_index=True)
                df_final.to_csv(DB_FILE, index=False)
                print("Datos añadidos al CSV existente.")
            else:
                # Si el archivo es de 1 Byte o no existe, creamos uno nuevo
                df_nuevo.to_csv(DB_FILE, index=False)
                print("CSV corrupto detectado. Se ha creado uno nuevo y limpio.")
        except Exception as e:
            # Ante cualquier error de lectura (como el EmptyDataError), sobreescribimos
            print(f"Error recuperado: {e}. Reemplazando archivo dañado...")
            df_nuevo.to_csv(DB_FILE, index=False)
