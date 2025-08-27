import os
from io import StringIO
from typing import Iterable

import pandas as pd

CANDIDATE_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]


class ProcesoInicial:
    """Carga y depura archivos de movimientos en formato CSV."""

    def __init__(self, input_path: str) -> None:
        self.input_path = input_path

    # ------------------------------------------------------------------
    def _detect_encoding(self, nbytes: int = 32768) -> str:
        """Detecta la codificación del archivo usando heurística simple."""
        with open(self.input_path, "rb") as fh:
            raw = fh.read(nbytes)
        for enc in CANDIDATE_ENCODINGS:
            try:
                raw.decode(enc)
                return enc
            except UnicodeDecodeError:
                continue
        return "latin-1"

    # ------------------------------------------------------------------
    def load_data(self) -> pd.DataFrame:
        """Lee el CSV, corrige comas extra y devuelve un :class:`DataFrame`."""
        lines: list[str] = []
        replaced = 0
        enc = self._detect_encoding()
        with open(self.input_path, "r", encoding=enc, errors="replace", newline="") as f:
            for ln in f:
                ln = ln.replace('"', "")
                if "CN LIFE, COMPA#IA" in ln:
                    ln = ln.replace("CN LIFE, COMPA#IA", "CN LIFE COMPA#IA")
                    replaced += 1
                lines.append(ln.rstrip("\n"))
        print(f"Reemplazadas comas en {replaced} líneas con 'CN Life,'")
        text = "\n".join(lines)
        sep = r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)'
        return pd.read_csv(StringIO(text), sep=sep, engine="python", on_bad_lines="warn")

    # ------------------------------------------------------------------
    @staticmethod
    def limpiar_numero(x: object) -> float:
        """Convierte valores con distintos formatos numéricos a ``float``."""
        if pd.isna(x):
            return 0.0
        s = str(x).strip()
        if "." in s and "," in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            if s.count(",") > 1:
                s = s[: s.rfind(",")] + s[s.rfind(",") + 1 :]
            s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    @staticmethod
    def filtrar_empresas(df: pd.DataFrame, lista: Iterable[str], include: bool = True) -> pd.DataFrame:
        mask = df["EMPRESA"].isin(lista)
        return df[mask] if include else df[~mask]

    @staticmethod
    def filtrar_instrumento(df: pd.DataFrame, lista: Iterable[str], include: bool = True) -> pd.DataFrame:
        mask = df["TIPO_INSTRUMENTO"].isin(lista)
        return df[mask] if include else df[~mask]

    @staticmethod
    def weighted_avg(values: pd.Series, weights: pd.Series) -> float:
        total = weights.sum()
        return float((values * weights).sum() / total) if total != 0 else 0.0

    # ------------------------------------------------------------------
    def generar_output(self) -> pd.DataFrame:
        """Genera el resumen filtrado y agrega promedios ponderados."""
        lista_empresas = [
            "4 LIFE SEGUROS DE VIDA S.A.",
            "BICE VIDA COMPAÑIA DE SEGUROS S.A.",
            "CN LIFE COMPA#IA DE SEGUROS DE VIDA S.A.",
            "COMPA#IA DE SEGUROS CONFUTURO S.A.",
            "COMPA#IA DE SEGUROS DE VIDA CONSORCIO NACIONAL",
            "METLIFE CHILE SEGUROS DE VIDA S.A.",
            "PENTA VIDA COMPA#IA DE SEGUROS DE VIDA S.A.",
            "PRINCIPAL COMPA#IA DE SEGUROS DE VIDA CHILE S.",
        ]
        lista_instrumentos = [
            "BB",
            "BBNEE",
            "BBP",
            "BNPNEE",
            "BCU",
            "BE",
            "BNEE",
            "BS",
            "BTP",
            "BTU",
            "CS",
        ]
        df = self.load_data()
        filtrado = self.filtrar_empresas(df, lista_empresas)
        filtrado = self.filtrar_instrumento(filtrado, lista_instrumentos)
        filtrado = filtrado[
            ~filtrado["METOD_CLASIF_VALORIZ_EEFF"].astype(str).str.contains("CUI", na=False)
        ].copy()
        cols_numericas = [
            "VALOR_NOMINAL",
            "VALOR_PRESENTE_TIR_COMPRA_MC",
            "VALOR_FINAL_MC",
            "TIR_COMPRA",
            "DURATION",
        ]
        for col in cols_numericas:
            filtrado[col] = filtrado[col].apply(self.limpiar_numero)
        cols_group = [
            "EMPRESA",
            "RUT_EMPRESA",
            "FECHA_CIERRE",
            "FECHA_COMPRA",
            "NRO_RUT",
            "TIPO_INSTRUMENTO",
            "NEMOTECNICO",
            "UNIDAD_MONETARIA",
            "CLAS_RIESGO",
            "FECHA_VENCIMIENTO",
            "TIR_COMPRA",
            "DURATION",
            "METOD_CLASIF_VALORIZ_EEFF",
            "CODIGO_OPERACION",
        ]
        resumen = (
            filtrado.groupby(cols_group, dropna=False)
            .apply(
                lambda g: pd.Series(
                    {
                        "VALOR_NOMINAL": g["VALOR_NOMINAL"].sum(),
                        "VALOR_PRESENTE_TIR_COMPRA_MC": g["VALOR_PRESENTE_TIR_COMPRA_MC"].sum(),
                        "VALOR_FINAL_MC": g["VALOR_FINAL_MC"].sum(),
                    }
                )
            )
            .reset_index()
        )
        resumen["TIR_COMPRA_POND"] = (
            filtrado.groupby(cols_group)["TIR_COMPRA"].transform(
                lambda x: self.weighted_avg(x, filtrado.loc[x.index, "VALOR_PRESENTE_TIR_COMPRA_MC"])
            )
        )
        resumen["DURATION_POND"] = (
            filtrado.groupby(cols_group)["DURATION"].transform(
                lambda x: self.weighted_avg(x, filtrado.loc[x.index, "VALOR_PRESENTE_TIR_COMPRA_MC"])
            )
        )
        return resumen
