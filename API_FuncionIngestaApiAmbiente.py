
@app.timer_trigger(schedule="0 0 0-23 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def FuncionIngestaApiAmbiente(myTimer: func.TimerRequest) -> None:
    
    # Define la URL de la API para obtener la lista de dispositivos conectados al servicio SwitchBot
    url_devices = "https://api.switch-bot.com/v1.1/devices"
    
    # Define los encabezados que se enviarán con la solicitud HTTP. 
    # En este caso, incluye un token de autorización (reemplazado aquí por "sdasdas").
    headers = {
        "Authorization": "Colocar codigo de Autorizacion",
        "t": "172461097068250",
        "sign": "Colocar codigo de Singal",
        "nonce": "Colocar codigo de nonce"
    }
    
    # Realiza una solicitud GET a la URL especificada para obtener la lista de dispositivos.
    # Se incluyen los encabezados, en particular, el token de autorización.
    response = requests.get(url_devices, headers=headers)
    
    # Verifica si la solicitud fue exitosa, es decir, si el código de estado HTTP es 200 (OK).
    if response.status_code == 200:
        # Si la solicitud fue exitosa, convierte la respuesta JSON en un diccionario de Python.
        devices = response.json()
    else:
        # Si la solicitud no fue exitosa, imprime un mensaje de error con el código de estado HTTP
        # y el contenido del cuerpo de la respuesta para obtener más detalles sobre el error.
        print(f"Error: {response.status_code} - {response.text}")
    
    # Define la zona horaria de -5 horas (por ejemplo, para la zona horaria de Bogotá, Lima, Quito)
    zona_horaria = timezone(timedelta(hours=-5))
    
    # Obtén la fecha y hora actual con la zona horaria definida (zona horaria de -5)
    fecha_actual_zona_horaria = datetime.now(zona_horaria)
    
    # Inicializa una lista vacía que almacenará la información de los dispositivos
    list_devices = list()
    
    # Itera sobre cada dispositivo en la lista 'deviceList' que se encuentra dentro de 'body' del objeto 'devices'
    for device in (devices['body'])['deviceList']:

        dict_device = dict()    
        dict_device['id_dispositivo'] = device['deviceId'] # ID del dispositivo en el diccionario    
        dict_device['nombre_dispositivo'] = device['deviceName'] # nombre del dispositivo en el diccionario
        
        # Construye la URL para obtener el estado actual del dispositivo específico usando su ID
        url_devices_status = f"https://api.switch-bot.com/v1.1/devices/{device['deviceId']}/status"
        
        #Realiza una espera
        te.sleep(1)

        # Realiza una solicitud GET a la API de SwitchBot para obtener el estado del dispositivo
        response = requests.get(url_devices_status, headers=headers)
        
        # Validar el código de estado de la respuesta
        if response.status_code == 200:

            # Convierte la respuesta en formato JSON en un diccionario de Python
            devices_status = response.json()

            dict_device['humedad'] = (devices_status['body'])['humidity']# Valor de la humedad del dispositivo    
            dict_device['temperatura'] = (devices_status['body'])['temperature'] # Valor de la temperatura del dispositivo
            dict_device['respuesta'] = devices_status['message'] # Mensaje de respuesta del sensor    
            dict_device['fechaConsulta'] = fecha_actual_zona_horaria.strftime('%Y-%m-%d %H:%M:%S') # Fecha y hora de la consulta

            # Añade el diccionario con la información del dispositivo a la lista de dispositivos
            list_devices.append(dict_device)
            
        else:
            
            #Registra el codigo de estatus de la solicitud
            logging.info(f'Error solicitud status_code:{str(response.status_code)}')
            
    df_nuevos = pd.DataFrame(list_devices)   
    
    # Cadena de conexión a tu cuenta de Azure Blob Storage
    cadena_conexion = ‘Conexión al conector de Azure Blod Storage’
    
    # Nombre del archivo
    nombre_archivo = f"Ambiente_residencial/fecha={fecha_actual_zona_horaria.strftime('%Y-%m-%d')}/Datos_ambiente_residencial.parquet"
    
    # Crear un cliente de BlobService
    blob_service_client = BlobServiceClient.from_connection_string(cadena_conexion)
    
    # Obtener el cliente del contenedor
    contenedor_cliente = blob_service_client.get_container_client('file')
    blob_cliente = contenedor_cliente.get_blob_client(nombre_archivo)
    
    # Comprobar si el archivo Parquet ya existe en el contenedor
    archivo_existe = blob_cliente.exists()
    
    # Leer el contenido existente si el archivo ya existe
    if archivo_existe:
        stream = io.BytesIO(blob_cliente.download_blob().readall())
        df_existente = pd.read_parquet(stream)
    else:
        df_existente = pd.DataFrame()  # DataFrame vacío si no existe el archivo
    
    # Combinar los datos existentes con los nuevos
    df_final = pd.concat([df_existente, df_nuevos])
    
    # Convertir el DataFrame a formato Parquet en un buffer
    buffer = io.BytesIO()
    df_final.to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)
    
    # Subir el archivo Parquet a Azure Blob Storage
    blob_cliente.upload_blob(buffer, overwrite=True)

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
