import pandas as pd
from src.io_utils import read_csv, save_csv
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'salario' not in df.columns:
        raise KeyError('Columna salario no encontrada')


    df['salario_mensual'] = df['salario'] / 12
    df['categoria_edad'] = pd.cut(df['edad'], bins=[-1, 25, 35, 200], labels=['Joven', 'Adulto', 'Senior'])
    return df.reset_index(drop=True)

if __name__ == '__main__':
    df = read_csv('data/processed/step2_types_fixed.csv')
    df_feat = add_features(df)
    save_csv(df_feat, 'data/processed/datos_limpios.csv')
    print(df_feat[['nombre', 'edad', 'categoria_edad', 'salario', 'salario_mensual']])