import logging
from src.create_dataset import create_example_csv 
from src.io_utils import read_csv, save_csv, ensure_dir
from src.clean_duplicates import drop_duplicates
from src.clean_types_formats import fix_types_and_format
from src.feature_engineering import add_features
from src.verify import verify


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s - %(message)s')
logger = logging.getLogger('pipeline')

def run(full_create_example: bool = False) -> dict:
    # Preparar carpetas
    ensure_dir('data/raw')
    ensure_dir('data/processed')

    # Paso 0: crear dataset de ejemplo
    if full_create_example:
        create_example_csv('data/raw/datos.csv')

    # Paso 1: carga
    df = read_csv('data/raw/datos.csv')
    logger.info('Loaded raw data with %d rows', len(df))

    # Paso 2: eliminar duplicados
    df = drop_duplicates(df)
    save_csv(df, 'data/processed/step1_no_duplicates.csv')

    # Paso 3: corregir tipos
    df = fix_types_and_format(df)
    save_csv(df, 'data/processed/step2_types_fixed.csv')

    # Paso 4: crear features
    df = add_features(df)
    save_csv(df, 'data/processed/datos_limpios.csv')

    # Paso 5: verificar 
    report = verify()
    logger.info('Pipeline finished')
    return report


if __name__ == '__main__':
    report = run(full_create_example=True)
    print('Reporte final:')
for k, v in report.items():
    print(f"{k}: {v}")


