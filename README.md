## **Ejercicio**: Pipeline completo de limpieza de datos realista
Ejercicio práctico para aplicar los conceptos aprendidos.

| Autor            | Fecha        | Día |
|------------------|--------------|----------|
| **Carlos Vásquez** |15 Noviembre 2025 | 3|

##  Crear entorno virtual
```sh
python -m venv analisis_datos_env
```

##  Activar entorno
analisis_datos_env\Scripts\activate

## Instalar las dependencia en el entorno virtual
- pandas
- numpy     

```bash
pip install pandas numpy
```
## Actualizar 
```bash
python.exe -m pip install --upgrade pip
```
## Respaldo de la instalación
```bash
pip freeze > requeriments.txt
``` 
## Estructura de archivos

```klotin
pipeline-limpieza/
├── data/
│ ├── raw/
│ │ └── datos.csv # CSV original con problemas
│ └── processed/
│ └── datos_limpios.csv # Salida limpia
├── src/
│ ├── create_dataset.py # Crea el CSV de ejemplo (opcional)
│ ├── inspect.py # Inspección y diagnóstico
│ ├── clean_duplicates.py # Eliminación de duplicados
│ ├── clean_types_formats.py # Corrección de tipos y formatos
│ ├── feature_engineering.py # Columnas calculadas (salario mensual, categoría edad)
│ ├── verify.py # Verificación final y comparación
│ └── utils.py # Funciones auxiliares reutilizables
├── notebooks/
│ └── cleaning_pipeline.ipynb # Notebook con ejecución paso a paso (opcional)
├── requirements.txt
└── README.md
```

## src/io_utils.py
```python
from pathlib import Path
import pandas as pd
import logging
from typing import Union

logger = logging.getLogger(__name__)

def ensure_dir(path: Union[str, Path]) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def read_csv(path: Union[str, Path]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        logger.error('File not found: %s', p)
        raise FileNotFoundError(p)
    return pd.read_csv(p)

def save_csv(df: pd.DataFrame, path: Union[str, Path]) -> None:
    p = Path(path)
    ensure_dir(p.parent)
    df.to_csv(p, index=False)
    logger.info('Saved CSV to %s', p)
```
## src/create_dataset.py
```python
from pathlib import Path
import pandas as pd
from src.io_utils import ensure_dir, save_csv


def create_example_csv(path: str = 'data/raw/datos.csv') -> None:
    datos = {
        'id': [1, 2, 3, 4, 5, 1, 6],
        'nombre': ['Ana García', 'Carlos López', 'María Rodríguez', 'Juan Pérez', 'Ana García', 'ana garcia', 'Luis Martín'],
        'edad': ['25', '30', '28', '35', '25', '25', '40'],
        'email': ['ana@email.com', 'carlos@email.com', 'maria@email.com', 'juan@email.com', 'ana@email.com', 'ana@email.com', 'luis@email.com'],
        'salario': [45000, 55000, 48000, 60000, 45000, 45000, 52000],
        'departamento': ['Ventas', 'IT', 'Marketing', 'IT', 'ventas', 'VENTAS', 'Recursos Humanos']
    }
    df = pd.DataFrame(datos)
    save_csv(df, path)

if __name__ == '__main__':  create_example_csv()
```
## src/inspect.py
```python
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
```
## src/clean_duplicates.py
```python
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
```
## src/clean_types_formats.py
```python
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
```
## src/feature_engineering.py
```python
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
```
## src/verify.py
```python
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
```
## src/run_pipeline.py
```python
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

```

## Ejecutar pipeline (crea el CSV de ejemplo y procesa):

```bash
python -m src.run_pipeline
```

### Si lo vas a utilizar y vez que no tienes el entorno analisis_datos_env o entorno virtual, puedes instalar las dependencias desde el respaldo para que funcione todo correctamente   

```sh
pip install -r requirements.txt
```