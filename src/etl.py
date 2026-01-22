import pandas as pd

def run_etl():
    """
    Implementa el proceso ETL.
    No cambies el nombre de esta función.
    """
    #Obtener datos del archivo
    df = pd.read_csv("data/citas_clinica.csv")

    #Normalizar los textos
    df = df[df['paciente'].notna() & (df['paciente'].str.strip() != '')]
    df['paciente'] = df['paciente'].str.title()
    df['especialidad'] = df['especialidad'].str.upper()

    #Convertir fechas y validacion de fechas
    df['Fecha_cita'] = pd.to_datetime(df['fecha_cita'],format='%Y-%m-%d', errors='coerce')
    df = df[df['Fecha_cita'].notna()]
    df['fecha_cita'] = df['Fecha_cita'].dt.strftime('%Y-%m-%d')
    df = df.drop('Fecha_cita', axis=1)

    #Verificar si el costo no sea <=0
    df = df[df['costo'] >=0]

    #Validar si nos números son validos, sino manda no registra
    df['telefono'] = df['telefono'].fillna('NO REGISTRA')
    df.loc[df['telefono'] == '','telefono'] = 'NO REGISTRA'

    #Ubicación de salida
    df.to_csv("data/output.csv", index=False)


if __name__ == "__main__":
    run_etl()
