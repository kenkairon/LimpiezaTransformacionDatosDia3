import logging
import pandas as pd
from src.io_utils import read_csv


logger = logging.getLogger(__name__)

def inspect_df(df: pd.DataFrame) -> dict:
    report = {
        'dtypes': df.dtypes.to_dict(),
        'duplicated_id_count': int(df['id'].duplicated().sum()),
        'duplicated_full_count': int(df.duplicated().sum()),
        'departamento_unique': df['departamento'].unique().tolist()
    }
    return report


if __name__ == '__main__':
    df = read_csv('data/raw/datos.csv')
    logger.info('Datos originales:')
    print(df)
r = inspect_df(df)
print('Informe de inspección:')
for k, v in r.items():
    print(f"{k}: {v}")