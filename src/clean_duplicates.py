import pandas as pd
from typing import Optional


def drop_duplicates(df: pd.DataFrame, subset=None, keep: str = 'first') -> pd.DataFrame:
    if subset is None:
        subset = ['id', 'email']
    return df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)


# Test-run when ejecutado directamente
if __name__ == '__main__':
    import pandas as pd
    from src.create_dataset import create_example_csv
    from src.io_utils import read_csv, save_csv


    create_example_csv('data/raw/datos.csv')
    df = read_csv('data/raw/datos.csv')
    df_clean = drop_duplicates(df)
    save_csv(df_clean, 'data/processed/step1_no_duplicates.csv')
    print(f"Filas después de drop_duplicates: {len(df_clean)}")