import pandas as pd
from typing import Optional


def fix_types_and_format(df: pd.DataFrame) -> pd.DataFrame:
    # Copiar para evitar side-effects
    df = df.copy()


    # Convertir edad a numérico
    df['edad'] = pd.to_numeric(df['edad'], errors='coerce')


    # Normalizar departamento: eliminar espacios y Title Case
    df['departamento'] = df['departamento'].astype(str).str.strip().str.title()


    # Normalizar nombres
    df['nombre'] = df['nombre'].astype(str).str.strip().str.title()


    # Normalizar emails
    df['email'] = df['email'].astype(str).str.strip().str.lower()


    return df.reset_index(drop=True)


if __name__ == '__main__':
    from src.io_utils import read_csv, save_csv


    df = read_csv('data/processed/step1_no_duplicates.csv')
    df_fixed = fix_types_and_format(df)
    save_csv(df_fixed, 'data/processed/step2_types_fixed.csv')
    print(df_fixed.head())