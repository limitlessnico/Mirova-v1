# 📊 Reporte Análisis Black Box

**Fecha:** $(date +%Y-%m-%d\ %H:%M:%S\ UTC)

## Resultados

Ver archivo: `eventos_perdidos_confirmados.csv`

## Estadísticas

```
$(if [ -f monitoreo_satelital/eventos_perdidos_confirmados.csv ]; then
  total=$(wc -l < monitoreo_satelital/eventos_perdidos_confirmados.csv)
  echo "Total eventos perdidos: $((total - 1))"
  echo ""
  echo "Desglose por volcán:"
  tail -n +2 monitoreo_satelital/eventos_perdidos_confirmados.csv | cut -d',' -f3 | sort | uniq -c | sort -rn
else
  echo "✅ No hay eventos perdidos"
fi)
```

## Conclusión

$(if [ -f monitoreo_satelital/eventos_perdidos_confirmados.csv ]; then
  echo "Se detectaron pérdidas de datos. Revisar archivo CSV para detalles."
else
  echo "✅ Sistema funcionando perfectamente - Captura al 100%"
fi)
