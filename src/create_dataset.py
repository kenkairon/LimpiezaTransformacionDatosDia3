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

