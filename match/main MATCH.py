import re
import ast
from pathlib import Path
from typing import Optional, Union
import pandas as pd
import pytz

def construir_excel_eeg(
   txt_path: Union[str, Path],
   csv_path: Union[str, Path],
   out_path: Union[str, Path],
   *,
   round_decimals: int = 4,
   tolerance_seconds: Optional[float] = None,
   sheet_name: str = "Datos",
) -> pd.DataFrame:

   txt_path = Path(txt_path)
   csv_path = Path(csv_path)
   out_path = Path(out_path)

   df = pd.read_csv(csv_path)
   df.columns = [c.strip() for c in df.columns]
   if "Timestamp" not in df.columns:
       raise ValueError("No se encontró la columna 'Timestamp' en el CSV.")
   original_cols = df.columns.tolist()
   
   text = txt_path.read_text(encoding="utf-8", errors="ignore")
   events = []
   for m in re.finditer(r"mc data:\s*(\{.*?\})", text, flags=re.DOTALL):
       d_str = m.group(1)
       try:
           d = ast.literal_eval(d_str)
           if "time" in d and "action" in d:
               events.append({"Timestamp": float(d["time"]), "Class": str(d["action"])})
       except Exception:

           pass
   df_txt = pd.DataFrame(events)
 
   if df_txt.empty:

       df_merged = df.copy()
       df_merged["Class"] = pd.NA
   else:
       if tolerance_seconds is None:

           df = df.copy()
           df_txt = df_txt.copy()
           df["ts_key"] = df["Timestamp"].round(round_decimals)
           df_txt["ts_key"] = df_txt["Timestamp"].round(round_decimals)
           df_txt = df_txt.sort_values("Timestamp").drop_duplicates(subset=["ts_key"], keep="first")
           df_merged = df.merge(df_txt[["ts_key", "Class"]], on="ts_key", how="left")
           df_merged.drop(columns=["ts_key"], inplace=True)
       else:
           df_sorted = df.sort_values("Timestamp").copy()
           txt_sorted = df_txt.sort_values("Timestamp").copy()
           df_merged = pd.merge_asof(
               df_sorted,
               txt_sorted[["Timestamp", "Class"]].rename(columns={"Timestamp": "txt_time"}),
               left_on="Timestamp",
               right_on="txt_time",
               tolerance=pd.Timedelta(seconds=float(tolerance_seconds)),
               direction="nearest", 
           )
           df_merged.drop(columns=["txt_time"], inplace=True)

   tz = pytz.timezone("America/Mexico_City")
   dt_utc = pd.to_datetime(df_merged["Timestamp"], unit="s", utc=True)
   dt_local = dt_utc.dt.tz_convert(tz)
   df_merged["Fecha"] = dt_local.dt.strftime("%Y-%m-%d")
   df_merged["Hora"] = dt_local.dt.strftime("%H:%M:%S.%f").str[:-3]
   new_cols = []
   for c in original_cols:
       new_cols.append(c)
       if c == "Timestamp":
           new_cols.extend(["Fecha", "Hora", "Class"])
   new_cols = [c for i, c in enumerate(new_cols) if c in df_merged.columns and c not in new_cols[:i]]
   df_final = df_merged.reindex(columns=new_cols)
   with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
       df_final.to_excel(writer, index=False, sheet_name=sheet_name)
   return df_final

df_resultado = construir_excel_eeg(
     txt_path="C:/Users/ez9709/Downloads/Python_Empujar y Jalar.txt",
     csv_path="C:/Users/ez9709/Downloads/Movimientos Empujar y jalar.csv",
     out_path="ResultadoFinal.xlsx",
     round_decimals=4,
 )
print("Filas:", len(df_resultado))