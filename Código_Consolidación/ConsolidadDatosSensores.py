
@app.timer_trigger(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def ConsolidadDatosSensores(myTimer: func.TimerRequest) -> None:

    # Conectar al BlobServiceClient usando la cadena de conexión
    connection_string = 'Conexión al conector de Azure Blod Storage'   
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Obtener el cliente del contenedor
    container_name = "file"
    container_client = blob_service_client.get_container_client(container_name)

    # Define la zona horaria de -5 horas
    zona_horaria = timezone(timedelta(hours=-5))

    # Obtén la fecha y hora actual con la zona horaria de -5
    fecha_actual_zona_horaria = datetime.now(zona_horaria)

    # Resta un día a la fecha y hora actual
    fecha_un_dia_menos = (fecha_actual_zona_horaria - timedelta(days=1)).date()

    # Ruta archivos de los sensores de Consumo electrico
    ruta_archivo_consumo_eletrico = f"Consumo_Electrico/fecha={fecha_un_dia_menos.strftime('%Y-%m-%d')}/Datos_Consumo_Electrico.parquet"

    # Descargar el blob
    blob_client = container_client.get_blob_client(ruta_archivo_consumo_eletrico)
    download_stream = blob_client.download_blob()

    # Lectura de archivos parquet 
    df_consumo_electrico = pd.read_parquet(io.BytesIO(download_stream.readall()))

    #Se crea un campo unico del nombre del canal (Nombre del Sensor)
    df_consumo_electrico['nombreCanalUnico'] = df_consumo_electrico['idCanal'] + ' - ' + df_consumo_electrico['nombreCanal']

    #Se homologa la hora
    df_consumo_electrico['fechaCaptura'] = pd.to_datetime(df_consumo_electrico['fechaCaptura'])
    df_consumo_electrico['fechaConsulta'] = pd.to_datetime(df_consumo_electrico['fechaConsulta'])
    df_consumo_electrico['fecha'] = df_consumo_electrico['fechaCaptura'].dt.date
    df_consumo_electrico['hora'] = df_consumo_electrico['fechaCaptura'].dt.time

    #Se seleccionas las variables para trabajar
    df_consumo_electrico_fil = df_consumo_electrico[['fecha','hora','nombreCanalUnico','usoCanal','fechaConsulta']]

    #Se coloca un id para identificar si existen mas de un registro dentro de una hora por cada sensor
    df_consumo_electrico_fil = df_consumo_electrico_fil.sort_values(by=['fecha','hora','nombreCanalUnico','fechaConsulta'], ascending=[True, True,True,True])
    df_consumo_electrico_fil['row_number'] = df_consumo_electrico_fil.groupby(['fecha','hora','nombreCanalUnico']).cumcount() + 1

    #Se filtrar todos las filas unicas, para dejar un valor para cada fecha, hora y sensor
    df_consumo_electrico_unico = df_consumo_electrico_fil[df_consumo_electrico_fil['row_number'] == 1]
    df_consumo_electrico_unico = df_consumo_electrico_unico.drop(columns=['row_number','fechaConsulta'])

    #Se realiza el Pivot de los datos
    df_consumo_electrico_pivot = df_consumo_electrico_unico.pivot(index=['fecha','hora'], columns='nombreCanalUnico', values='usoCanal').reset_index()
    df_consumo_electrico_pivot.columns.names = [None]

    #Renombrado de variables
    df_consumo_electrico_pivot.rename(columns={'fecha': 'Fecha'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'hora': 'Hora'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'1,2,3 - Casa #1': 'Consumo_Total_kWhs'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'Balance - Balance': 'Balance'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'1 - Refrigeradora': 'Refrigeradora'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'11 - Tomacorrientes Cocina': 'Tomacorrientes_Cocina'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'3 - Tomacorrientes P/B y lavadora': 'Tomacorriente_PB_y_Lavadora'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'2 - Tomacorrientes P/A': 'Tomacorriente_PA'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'16 - A.A Cuarto 3': 'AireAcondicionado_Oficina'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'9 - iluminación P/B': 'Iluminacion_PB'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'10 - iluminación P/A': 'Iluminacion_PA'}, inplace=True)
    df_consumo_electrico_pivot.rename(columns={'8 - Bomba de agua': 'Bomba_Agua'}, inplace=True)

    #Creacion de campos calculados
    df_consumo_electrico_pivot['Cocina_Induccion'] = df_consumo_electrico_pivot['12 - Cocina de Inducción'] + df_consumo_electrico_pivot['13 - Cocina de Inducción']
    df_consumo_electrico_pivot['AireAcondicionado_Cuarto1'] = df_consumo_electrico_pivot['4 - A.A cuarto 1'] + df_consumo_electrico_pivot['5 - A. A cuarto 1']
    df_consumo_electrico_pivot['AireAcondicionado_Cuarto2'] = df_consumo_electrico_pivot['6 - A.A cuarto 2'] + df_consumo_electrico_pivot['7 - A.A cuarto 2']
    df_consumo_electrico_pivot['AireAcondicionado_Sala'] = df_consumo_electrico_pivot['14 - A.A Sala'] + df_consumo_electrico_pivot['15 - A.A Sala']

    #Se filtran las columnas requeridas 
    df_consumo_electrico_fin = df_consumo_electrico_pivot[[
        'Fecha',
        'Hora',
        'Consumo_Total_kWhs',
        'Refrigeradora',
        'Tomacorrientes_Cocina',
        'Cocina_Induccion',  
        'Tomacorriente_PB_y_Lavadora',
        'Tomacorriente_PA',
        'AireAcondicionado_Cuarto1',
        'AireAcondicionado_Cuarto2',
        'AireAcondicionado_Oficina',
        'AireAcondicionado_Sala',
        'Iluminacion_PB',
        'Iluminacion_PA',
        'Bomba_Agua',
        'Balance'
    ]]

    # Ruta archivos de los sensores ambientales temperatura y Humdad
    ruta_archivo_ambiente_residencial = f"Ambiente_residencial/fecha={fecha_un_dia_menos.strftime('%Y-%m-%d')}/Datos_ambiente_residencial.parquet"

    # Descargar el blob
    blob_client = container_client.get_blob_client(ruta_archivo_ambiente_residencial)
    download_stream = blob_client.download_blob()

    # Lectura de archivos parquet 
    df_ambiente_residencial = pd.read_parquet(io.BytesIO(download_stream.readall()))

    #Se crea un campo unico del nombre del canal (Nombre del Sensor)
    df_ambiente_residencial['nombre_dispositivo_Unico'] = df_ambiente_residencial['id_dispositivo'] + ' - ' + df_ambiente_residencial['nombre_dispositivo']

    #Se homologa la hora
    df_ambiente_residencial['fechaConsulta'] = pd.to_datetime(df_ambiente_residencial['fechaConsulta'])
    df_ambiente_residencial['fecha'] = df_ambiente_residencial['fechaConsulta'].dt.date
    df_ambiente_residencial['hora'] = df_ambiente_residencial['fechaConsulta'].dt.floor('h').dt.time

    #Se seleccionas las variables para trabajar
    df_ambiente_residencial_fil = df_ambiente_residencial[['fecha','hora','nombre_dispositivo_Unico','humedad','temperatura','fechaConsulta']]

    #Se coloca un id para identificar si existen mas de un registro dentro de una hora por cada sensor
    df_ambiente_residencial_fil = df_ambiente_residencial_fil.sort_values(by=['fecha','hora','nombre_dispositivo_Unico','fechaConsulta'], ascending=[True, True,True,True])
    df_ambiente_residencial_fil['row_number'] = df_ambiente_residencial_fil.groupby(['fecha','hora','nombre_dispositivo_Unico']).cumcount() + 1

    #Se filtrar todos las filas unicas, para dejar un valor para cada fecha, hora y sensor
    df_ambiente_residencial_unico = df_ambiente_residencial_fil[df_ambiente_residencial_fil['row_number'] == 1]
    df_ambiente_residencial_unico = df_ambiente_residencial_unico.drop(columns=['row_number','fechaConsulta'])

    #Se realiza el Pivot de los datos
    df_ambiente_residencial_pivot = df_ambiente_residencial_unico.pivot(index=['fecha','hora'], columns='nombre_dispositivo_Unico', values=['humedad','temperatura']).reset_index()
    #Se aplanan los indices de humedad y temperatura
    df_ambiente_residencial_pivot.columns = ['_'.join(col).strip() for col in df_ambiente_residencial_pivot.columns.values]

    #Renombrado de variables
    df_ambiente_residencial_pivot.rename(columns={'fecha_': 'Fecha'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'hora_': 'Hora'}, inplace=True)   
    df_ambiente_residencial_pivot.rename(columns={'temperatura_C2D56D6DBDB1 - Principal': 'Sala_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_C2D56D6DBDB1 - Principal': 'Sala_Humedad_Relativa_%'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'temperatura_C862520082B3 - Patio exterior (1)': 'PatioLateral_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_C862520082B3 - Patio exterior (1)': 'PatioLateral_Humedad_Relativa_%'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'temperatura_E329637491AB - Exterior Fachada (4)': 'ExteriorFachada_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_E329637491AB - Exterior Fachada (4)': 'ExteriorFachada_Humedad_Relativa_%'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'temperatura_FFE630B37A2A - Cuarto principal (3)': 'CuartoPrincipal_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_FFE630B37A2A - Cuarto principal (3)': 'CuartoPrincipal_Humedad_Relativa_%'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'temperatura_F7777783A7E3 - Cocina (2)': 'Cocina_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_F7777783A7E3 - Cocina (2)': 'Cocina_Humedad_Relativa_%'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'temperatura_E2270EF14D2C - Patio trasero': 'PatioTrasero_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_E2270EF14D2C - Patio trasero': 'PatioTrasero_Humedad_Relativa_%'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'temperatura_D21A0705804C - Oficina': 'Oficina_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_D21A0705804C - Oficina': 'Oficina_Humedad_Relativa_%'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'temperatura_F85F0E44D64F - Cuarto # 2': 'Cuarto2_Temperatura_C'}, inplace=True)
    df_ambiente_residencial_pivot.rename(columns={'humedad_F85F0E44D64F - Cuarto # 2': 'Cuarto2_Humedad_Relativa_%'}, inplace=True)

    #Se filtran las columnas requeridas 
    df_ambiente_residencial_fin = df_ambiente_residencial_pivot[[
        'Fecha','Hora',
        'CuartoPrincipal_Temperatura_C','CuartoPrincipal_Humedad_Relativa_%',
        'Cuarto2_Temperatura_C','Cuarto2_Humedad_Relativa_%',
        'Oficina_Temperatura_C','Oficina_Humedad_Relativa_%',
        'Sala_Temperatura_C','Sala_Humedad_Relativa_%',
        'Cocina_Temperatura_C','Cocina_Humedad_Relativa_%',    
        'PatioLateral_Temperatura_C','PatioLateral_Humedad_Relativa_%',
        'PatioTrasero_Temperatura_C','PatioTrasero_Humedad_Relativa_%',
        'ExteriorFachada_Temperatura_C','ExteriorFachada_Humedad_Relativa_%'    
    ]]

    #Genera el rango de fechas y horas a procesar (las 24 horas del día)
    fechas_horas = pd.date_range(start=f"{fecha_un_dia_menos} 00:00", end=f"{fecha_un_dia_menos} 23:00", freq='h')

    #Se crea un dataframe base para consolidar los datos de los sensores
    df_base = pd.DataFrame({
        "datetime": fechas_horas
    })
    df_base['Fecha'] = df_base['datetime'].dt.date
    df_base['Hora'] = df_base['datetime'].dt.time
    df_base = df_base.drop(columns=['datetime'])

    #Se realizan los Join para tener todos los datos en un dataframe
    df_base = df_base.merge(df_consumo_electrico_fin, on=["Fecha", "Hora"], how="left")
    df_base = df_base.merge(df_ambiente_residencial_fin, on=["Fecha", "Hora"], how="left")

    #Creacion de variables para particionamiento del archivo parquet
    df_base['Anio'] = pd.to_datetime(df_base['Fecha']).dt.year
    df_base['Mes'] = pd.to_datetime(df_base['Fecha']).dt.month
    df_base['Dia'] = pd.to_datetime(df_base['Fecha']).dt.day

    #Seleccion de las variables para la estructura del archivo parquet
    df_base_pivot = df_base[[
        'Anio', 'Mes', 'Dia', 'Fecha', 'Hora', 'Consumo_Total_kWhs', 'Refrigeradora',
        'Tomacorrientes_Cocina', 'Cocina_Induccion',
        'Tomacorriente_PB_y_Lavadora', 'Tomacorriente_PA',
        'AireAcondicionado_Cuarto1', 'AireAcondicionado_Cuarto2',
        'AireAcondicionado_Oficina', 'AireAcondicionado_Sala', 'Iluminacion_PB',
        'Iluminacion_PA', 'Bomba_Agua', 'Balance',
        'CuartoPrincipal_Temperatura_C', 'CuartoPrincipal_Humedad_Relativa_%',
        'Cuarto2_Temperatura_C', 'Cuarto2_Humedad_Relativa_%',
        'Oficina_Temperatura_C', 'Oficina_Humedad_Relativa_%',
        'Sala_Temperatura_C', 'Sala_Humedad_Relativa_%', 'Cocina_Temperatura_C',
        'Cocina_Humedad_Relativa_%', 'PatioLateral_Temperatura_C',
        'PatioLateral_Humedad_Relativa_%', 'PatioTrasero_Temperatura_C',
        'PatioTrasero_Humedad_Relativa_%', 'ExteriorFachada_Temperatura_C',
        'ExteriorFachada_Humedad_Relativa_%'
    ]]

    #Creacion de df para visualizacion
    df_base_unpivot = pd.melt(df_base_pivot, id_vars=['Anio','Mes','Dia','Fecha','Hora'], var_name='Sensor', value_name='Medicion')

    #Guardado de datos para la visualizacion
    anio = fecha_un_dia_menos.year
    mes = fecha_un_dia_menos.month
    dia = fecha_un_dia_menos.day

    blob_path = f'Medicion_Sensores_V.parquet/Anio={anio}/Mes={mes}/Dia={dia}/Medicion_Sensores_V_{anio}-{mes}-{dia}.parquet'
    container_name = "cur"

    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_path)

    # Convertir el DataFrame a formato Parquet en un buffer
    buffer = io.BytesIO()
    df_base_unpivot.to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)

    # Subir el archivo Parquet a Azure Blob Storage
    blob_client.upload_blob(buffer, overwrite=True)

    #Guardado de datos para el modelo
    anio = fecha_un_dia_menos.year
    mes = fecha_un_dia_menos.month
    dia = fecha_un_dia_menos.day

    blob_path = f'cur/Medicion_Sensores_M.parquet/Anio={anio}/Mes={mes}/Dia={dia}/Medicion_Sensores_M_{anio}-{mes}-{dia}.parquet'

    blob_client = container_client.get_blob_client(blob_path)

    # Convertir el DataFrame a formato Parquet en un buffer
    buffer = io.BytesIO()
    df_base_pivot.to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)

    # Subir el archivo Parquet a Azure Blob Storage
    blob_client.upload_blob(buffer, overwrite=True)

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
