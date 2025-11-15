from src.io_utils import read_csv

def verify(original_path: str = 'data/raw/datos.csv', cleaned_path: str = 'data/processed/datos_limpios.csv') -> dict:
    df_orig = read_csv(original_path)
    df_clean = read_csv(cleaned_path)

    report = {
        'filas_originales': len(df_orig),
        'filas_limpias': len(df_clean),
        'duplicados_id_en_limpio': int(df_clean['id'].duplicated().sum()),
        'tipos_limpios': df_clean.dtypes.apply(lambda x: x.name).to_dict()
    }
    return report

if __name__ == '__main__':
    r = verify()
    print('Informe de verificación:')
    for k, v in r.items():
        print(f"{k}: {v}")