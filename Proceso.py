"""Herramientas para comparar movimientos entre dos meses."""
from __future__ import annotations

import os

from Inicio import ProcesoInicial

BASE_DIR = r"D:/OneDrive - consorcio.cl/Escritorio/Movimientos"


def comparar_meses(df_actual, df_anterior, mes_ant: str = "Junio"):
    """Compara dos periodos y calcula diferencias de valores."""
    claves = ["EMPRESA", "NEMOTECNICO", "TIPO_INSTRUMENTO", "UNIDAD_MONETARIA"]
    df_ant = df_anterior[claves + ["VALOR_NOMINAL", "VALOR_FINAL_MC"]].copy()
    df_ant = df_ant.rename(
        columns={
            "VALOR_NOMINAL": f"VALOR_NOMINAL_{mes_ant}",
            "VALOR_FINAL_MC": f"VALOR_FINAL_MC_{mes_ant}",
        }
    )
    df = df_actual.merge(df_ant, on=claves, how="left")
    df[f"DELTA_VALOR_NOMINAL"] = df["VALOR_NOMINAL"] - df[f"VALOR_NOMINAL_{mes_ant}"].fillna(0)
    df[f"DELTA_VALOR_FINAL"] = df["VALOR_FINAL_MC"] - df[f"VALOR_FINAL_MC_{mes_ant}"].fillna(0)
    df = df[df[f"DELTA_VALOR_NOMINAL"] != 0].copy()
    return df


def main() -> None:
    d1 = input("Ingresa la fecha del archivo INICIAL (Ej: 1124): ")
    d2 = input("Ingresa la fecha del archivo FINAL (Ej: 0625): ")
    filename = os.path.join(BASE_DIR, f"v_B1_{d1}.csv")
    filename2 = os.path.join(BASE_DIR, f"v_B1_{d2}.csv")
    mes_final = ProcesoInicial(filename).generar_output()
    print("Mes Final Depurado")
    mes_inicial = ProcesoInicial(filename2).generar_output()
    print("Mes Inicial Depurado")
    df_resultado = comparar_meses(mes_final, mes_inicial, mes_ant=d1)
    print("Comparando meses")
    print(df_resultado.head())
    output = os.path.join(BASE_DIR, "prueba.xlsx")
    df_resultado.to_excel(output, index=False, engine="xlsxwriter")


if __name__ == "__main__":
    main()
