@app.timer_trigger(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def FuncionIngestaAPIConsumoElectrico(myTimer: func.TimerRequest) -> None:

    # Inicializa PyEmVue y se autentica en la cuenta Emporia Vue
    # Crea una instancia del cliente PyEmVue
    vue = pyemvue.PyEmVue()
    # Inicia sesión en Emporia Vue con las credenciales 
    vue.login(username='Correo de la Cuenta', password='Contraseña')

    # Obtiene la lista de dispositivos conectados a la cuenta Emporia Vue
    devices = vue.get_devices()
    device_gids = [] # Lista para almacenar los GIDs de los dispositivos
    device_info = {} # Diccionario para almacenar información detallada de cada dispositivo

    # Procesa cada dispositivo recuperado
    for device in devices:
        if not device.device_gid in device_gids:
            device_gids.append(device.device_gid) # Añade el GID del dispositivo si no está en la lista
            device_info[device.device_gid] = device # Almacena la información del dispositivo en el diccionario
        else:
            device_info[device.device_gid].channels += device.channels # Si el dispositivo ya está en la lista, añade sus canales

    # Define la zona horaria de -5 horas
    zona_horaria = timezone(timedelta(hours=-5))

    # Obtén la fecha y hora actual con la zona horaria de -5
    fecha_actual_zona_horaria = datetime.now(zona_horaria)

    # Resta un día a la fecha y hora actual
    fecha_un_dia_menos = fecha_actual_zona_horaria - timedelta(days=1)

    # Define la hora de inicio del día en la zona horaria -5
    inicio_del_dia = datetime(fecha_un_dia_menos.year, fecha_un_dia_menos.month, fecha_un_dia_menos.day, 0, 0, tzinfo=timezone(timedelta(hours=-5)))

    # Inicializa una lista para almacenar los datos del reporte
    list_reporte = list()
    # Itera sobre cada hora del día
    for i in range(0, 24):  
        hora = inicio_del_dia + timedelta(minutes=60 * i) # Define la hora de la consulta
        # Obtiene el uso de energía de cada dispositivo para la hora especificada
        device_usage_dict = vue.get_device_list_usage(deviceGids=device_gids, instant=hora, scale=Scale.HOUR.value, unit=Unit.KWH.value)

        # Inicializa una lista para almacenar los registros de la hora actual
        list_log = list()    
        for gid, device in device_usage_dict.items():
            for channelnum, channel in device.channels.items():
                name = channel.name # Obtiene el nombre del canal
                if name == 'Main': # Si el nombre del canal es 'Main', se usa el nombre del dispositivo
                    name = device_info[gid].device_name
                # Crea un diccionario para almacenar los datos del canal
                dict_log = dict()
                dict_log['fechaCaptura'] = hora.strftime('%Y-%m-%d %H:%M:%S') # Fecha y hora de la captura de datos
                dict_log['idDispositivo'] = gid # ID del dispositivo
                dict_log['idCanal'] = channelnum # Número del canal
                dict_log['nombreCanal'] = name # Nombre del canal
                dict_log['usoCanal'] = channel.usage # Uso del canal en kWh
                dict_log['fechaConsulta'] = fecha_actual_zona_horaria.strftime('%Y-%m-%d %H:%M:%S') # Fecha y hora de la consulta
                list_log.append(dict_log) # Añade el registro a la lista de logs
        list_reporte = list_reporte+list_log # Añade todos los registros de la hora actual al reporte general

    # Convierte la lista de registros en un DataFrame de pandas
    df = pd.DataFrame(list_reporte)

    # Configuración de Azure Blob Storage
    # Cadena de conexión a tu cuenta de Azure Blob Storage
    connect_str = 'Conexión al conector de Azure Blod Storage'    # Inicializa el cliente de servicio Blob con la cadena de conexión
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Ahora convertir este DataFrame a Parquet y subirlo de nuevo a Blob Storage
    parquet_buffer = io.BytesIO() # Crea un buffer en memoria para almacenar los datos del archivo Parquet
    df.to_parquet(parquet_buffer, index=False) # Convierte el DataFrame a formato Parquet y lo escribe en el buffer
    parquet_buffer.seek(0)  # Regresa al inicio del buffer para la lectura

    # Define el nombre del archivo Parquet, incluyendo la fecha en el nombre
    nombre_archivo_parquet = f"Consumo_Electrico/fecha={inicio_del_dia.strftime('%Y-%m-%d')}/Datos_Consumo_Electrico.parquet"

    # Crear un cliente de blob para el archivo Parquet
    parquet_blob_client = blob_service_client.get_blob_client(container='file', blob=nombre_archivo_parquet)

    # Subir el contenido del buffer del Parquet al blob
    parquet_blob_client.upload_blob(parquet_buffer, blob_type="BlockBlob", overwrite=True) # Sube el archivo Parquet al Blob Storage y sobrescribe si ya existe

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

