import pandas as pd
from io import StringIO

import re
import os, sys, runpy
import csv, codecs
CANDIDATE_ENCODINGS = ["utf-8-sig","utf-8","cp1252","latin-1"]

class Proceso_inicial:
    
    def __init__(self,input_path):
        self.input_path = os.path.join(input_path)
        
        
    def _detect_encoding(self, nbytes=32768):
        encs = ["utf-8-sig","utf-8","cp1252","latin-1"]
        raw = open(self.input_path, "rb").read(nbytes)
        for e in encs:
            try:
                raw.decode(e)
                return e
            except UnicodeDecodeError:
                continue
        return "latin-1"
            

    def load_data(self,path):
        """
        Lee el CSV en `path`, corrige filas donde el texto "CN Life,"
        introduzca una coma extra dentro del campo EMPRESA, y devuelve
        un DataFrame limpio.
        """
        lines = []
        replaced = 0
        enc = self._detect_encoding()
        with open(path, 'r', encoding=enc, errors='replace', newline='') as f:
            for ln in f:
                ln = ln.replace('"', '')  # quita TODAS las comillas
                # corrige la coma extra en CN LIFE
                if 'CN LIFE, COMPA#IA' in ln:
                    ln = ln.replace('CN LIFE, COMPA#IA', 'CN LIFE COMPA#IA')
                    replaced +=1
                lines.append(ln.rstrip('\n'))
               
    
        print(f"Reemplazadas comas en {replaced} líneas con 'CN Life,'")
        
    
        # arma el texto completo y parsea con regex que sólo parte en comas fuera de comillas
        text = "\n".join(lines)
        sep = r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
        return pd.read_csv(
            StringIO(text),
            sep=sep,
            engine='python',
            on_bad_lines='warn'
        )
    
    def limpiar_numero(self,x):
        if pd.isna(x):
            return 0.0
        s = str(x).strip()
    
        # caso: tiene separador de miles "." y decimal ","
        if "." in s and "," in s:
            s = s.replace(".", "").replace(",", ".")
        # caso: solo coma -> decimal
        elif "," in s:
            # si hay más de una coma, borro todas salvo la última
            if s.count(",") > 1:
                s = s[:s.rfind(",")] + s[s.rfind(",")+1:]
            s = s.replace(",", ".")
        # caso: solo puntos -> decimal, no tocar
        else:
            pass  
    
        try:
            return float(s)
        except:
            return 0.0
    
    
    def filtrar_empresas(self,df, lista_empresas, include=True):
    
       
        mask = df['EMPRESA'].isin(lista_empresas)
        return df[mask] if include else df[~mask]
    
    def filtrar_instrumento(self,df,lista_instrumentos,include=True):
        
        mask = df["TIPO_INSTRUMENTO"].isin(lista_instrumentos)
        return df[mask] if include else df[~mask]
    
    def weighted_avg(self,values, weights):
       
        total_weights = weights.sum()
        return (values * weights).sum() / total_weights if total_weights != 0 else 0
    
    def generar_output(self):
       
        
        lista_empresas = ['4 LIFE SEGUROS DE VIDA S.A.', 
                         
                          'BICE VIDA COMPAÑIA DE SEGUROS S.A.', 
                         
                          'CN LIFE COMPA#IA DE SEGUROS DE VIDA S.A.', 
                          'COMPA#IA DE SEGUROS CONFUTURO S.A.',
                          
                          'COMPA#IA DE SEGUROS DE VIDA CONSORCIO NACIONAL',
                         
                          'METLIFE CHILE SEGUROS DE VIDA S.A.',
                         
                          'PENTA VIDA COMPA#IA DE SEGUROS DE VIDA S.A.', 
                          'PRINCIPAL COMPA#IA DE SEGUROS DE VIDA CHILE S.', 
                         ]
        
        lista_instrumentos = ["BB", "BBNEE", "BBP","BNPNEE", "BCU", "BE", "BNEE", "BS", "BTP", "BTU", "CS"]
       
        output_path = "D:/OneDrive - consorcio.cl/Escritorio/Movimientos/output.xlsx"
        df = self.load_data(self.input_path)
    
        filtrado = self.filtrar_empresas(df,lista_empresas)
        unique_fil = filtrado1['EMPRESA'].unique().tolist()
   
        filtrado2 = self.filtrar_instrumento(filtrado,lista_instrumentos)
        filtrado = filtrado2.copy()
        filtrado = filtrado[~filtrado['METOD_CLASIF_VALORIZ_EEFF'].astype(str).str.contains('CUI', na=False)].copy()
        
        cols_numericas = [
            "VALOR_NOMINAL", 
            "VALOR_PRESENTE_TIR_COMPRA_MC", 
            "VALOR_FINAL_MC", 
            "TIR_COMPRA", 
            "DURATION"
        ]
        # 2) Lista de columnas a modificar
        for col in cols_numericas:
            filtrado[col] = filtrado[col].apply(self.limpiar_numero)
    
        cols_group = ["EMPRESA","RUT_EMPRESA","FECHA_CIERRE","FECHA_COMPRA","NRO_RUT","TIPO_INSTRUMENTO","NEMOTECNICO","UNIDAD_MONETARIA","CLAS_RIESGO","FECHA_VENCIMIENTO","TIR_COMPRA","DURATION","METOD_CLASIF_VALORIZ_EEFF","CODIGO_OPERACION"]
    # Faltan vs la otra lista: DURATION, PLAZO_AL_VCTO_M, TIPO_AMORTIZA, SUBYACENTE, EFECTO_PATRIMONIO, INCREMENTO_RIESGO, DEUDA_GARANTIA_OTOR, DEUDA_GARANTIA_VIG, CLAS_BANCO_LIDER, FIN_CLEAS.
        resumen = (
            filtrado.groupby(cols_group, dropna=False)
            .apply(lambda g: pd.Series({
                "VALOR_NOMINAL": g["VALOR_NOMINAL"].sum(),
                "VALOR_PRESENTE_TIR_COMPRA_MC": g["VALOR_PRESENTE_TIR_COMPRA_MC"].sum(),
                "VALOR_FINAL_MC": g["VALOR_FINAL_MC"].sum()
            }))
            .reset_index()
        ) 
        
        resumen["TIR_COMPRA_POND"] = (
            filtrado.groupby(cols_group)["TIR_COMPRA"]
            .transform(lambda x: self.weighted_avg(x, filtrado.loc[x.index, "VALOR_PRESENTE_TIR_COMPRA_MC"]))
            )

        resumen["DURATION_POND"] = (
            filtrado.groupby(cols_group)["DURATION"].transform(lambda x: self.weighted_avg(x, filtrado.loc[x.index, "VALOR_PRESENTE_TIR_COMPRA_MC"]))
            )
       # resumen.to_excel(output_path, index=False, engine='xlsxwriter')
        
        return resumen
