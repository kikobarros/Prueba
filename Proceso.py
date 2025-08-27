"""
Created on Fri Aug 22 13:21:36 2025

@author: fbarros
"""
import sys; sys.path.append(r"D:/OneDrive - consorcio.cl/Escritorio/Movimientos")
from Inicio import Proceso_inicial as p
import os
import io


d1 = input("Ingresa los la fecha correspondiente al archivo INICIAL (Ej: 1124): ")
d1 = "v_B1_" + d1 + ".csv"
d2 = input("Ingresa los la fecha correspondiente al archivo FINAL (Ej: 0625): ")
d2 = "v_B1_" + d2 + ".csv"

filename = os.path.join("D:/OneDrive - consorcio.cl/Escritorio/Movimientos",d1)
filename2 = os.path.join("D:/OneDrive - consorcio.cl/Escritorio/Movimientos",d2)

#filename = "C:/Users/kikob/consorcio.cl/Bloomberg - Estudios + Riesgo - Documentos/FBV/Movimientos - copia/v_B1_0625.csv"
#filename2 = "C:/Users/kikob/consorcio.cl/Bloomberg - Estudios + Riesgo - Documentos/FBV/Movimientos - copia/v_B1_1124.csv"

mes_final = p(filename).generar_output()
print("Mes Final Depurado")
mes_inicial = p(filename2).generar_output()
print("Mes Inicial Depurado")

def comparar_meses(df_actual, df_anterior, mes_ant="Junio"):
    # columnas clave para hacer el merge
    claves = ["EMPRESA", "NEMOTECNICO", "TIPO_INSTRUMENTO", "UNIDAD_MONETARIA"]

    # nos quedamos solo con las columnas necesarias
    df_ant = df_anterior[claves + ["VALOR_NOMINAL", "VALOR_FINAL_MC"]].copy()
    df_ant = df_ant.rename(columns={
        "VALOR_NOMINAL": f"VALOR_NOMINAL_{mes_ant}",
        "VALOR_FINAL_MC": f"VALOR_FINAL_MC_{mes_ant}"
    })

    # merge con el mes actual
    df = df_actual.merge(df_ant, on=claves, how="left")

    # calcular diferencias
    df[f"DELTA_VALOR_NOMINAL"] = df["VALOR_NOMINAL"] - df[f"VALOR_NOMINAL_{mes_ant}"].fillna(0)
    df[f"DELTA_VALOR_FINAL"]   = df["VALOR_FINAL_MC"] - df[f"VALOR_FINAL_MC_{mes_ant}"].fillna(0)
    
    df = df[df[f"DELTA_VALOR_NOMINAL"] != 0].copy()

    return df



df_resultado = comparar_meses(mes_final, mes_inicial, mes_ant=d1)
print("Comparando meses")
print(df_resultado.head())
output = "D:/OneDrive - consorcio.cl/Escritorio/Movimientos/prueba.xlsx"
df_resultado.to_excel(output, index=False, engine='xlsxwriter')
      



