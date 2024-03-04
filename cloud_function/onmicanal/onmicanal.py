#####################################
# Cloud Functions API - Omnicanal
# Edgar Beltrán Alvarado - Minsait
# 2024-01-04
#####################################
import requests
import json
import urllib3
import pandas as pd
import os
import google.auth
from google.cloud import storage
import google.cloud.logging
from google.logging.type import log_severity_pb2 as severity
import time
import datetime
from datetime import datetime, timedelta, timezone,date
# Método para validación de HTTPS
urllib3.disable_warnings()

#Configuración logs
def configuracion_log_cliente():
    
    #VARIABLE GLOBAL
    global logger

    # Configuración del logging cliente GLOBAL
    logging_client = google.cloud.logging.Client()
    logger = logging_client.logger("cfu-omnicanal-api-1a-gen")

# Función para validar que exista información de la(s) variable(s) en secret manager
def valid_content(external_variable,name_variable):
    # external_variable == VARIABLES EXTERNAS PARAMETRIZADAS O VARIABLES SECRET MANAGER
    # name_variable ====== NOMBRE DE LA VARIABLE QUE APARECERÁ EN EL LOG
    if external_variable == "NOT_CONFIGURED":
       logger.log_text("ERROR 400 en la variable: " + name_variable, severity=severity.INFO)   
    return external_variable

# Método con los parametros externos de la configuración de variables
def parametros_externos():

    # VARIABLES GLOBALES    
    global p_project_id                   
    global p_landing_bucket_name          
    global p_output_c_invitado_path_name  
    global p_output_c_registrado_path_name
    global p_output_c_inactivo_path_name

    global p_url_invitado_name                      
    global p_url_clientes_invitados_name    
    global p_url_clientes_registrados_name  
    global p_url_clientes_inactivos_name   

    global p_fecha_consultar   

    # PARAMETROS BUCKET
    p_project_id                    = os.environ['PROJECT_ID']                    
    p_landing_bucket_name           = os.environ['LANDING_BUCKET_NAME']
    p_delta_history                 = os.environ['DELTA_PATH']
    p_output_c_invitados_name       = os.environ['CLIENTE_INVITADO_NAME']   
    p_output_c_registrados_name     = os.environ['CLIENTE_REGISTRADO_NAME'] 
    p_output_c_inactivos_name       = os.environ['CLIENTE_INACTIVO_NAME']
    
    # PARAMETROS URL OMNICANAL
    p_url_invitado_name             = os.environ['URL_INVITADO_NAME']        
    p_url_clientes_invitados_name   = os.environ['URL_CLIENTES_INVITADOS_NAME'] 
    p_url_clientes_registrados_name = os.environ['URL_CLIENTES_REGISTRADOS_NAME']
    p_url_clientes_inactivos_name   = os.environ['URL_CLIENTES_INACTIVOS_NAME'] 
    
    #  PARAMETRO FECHA
    p_fecha_consultar               = os.environ['FECHA_CONSULTAR']
    #p_fecha_consultar = valid_content(os.environ.get('FECHA_CONSULTAR', 'NOT_CONFIGURED') ,    "FECHA_CONSULTAR")
    # COMENTAR LA SIGUIENTE LINEA SI SE DESEA UTILIZAR LA EJECUCIÓN AUTOMATICA
    p_output_c_invitado_path_name    = p_delta_history+"/" + p_output_c_invitados_name   +"/"
    p_output_c_registrado_path_name  = p_delta_history+"/" + p_output_c_registrados_name +"/"
    p_output_c_inactivo_path_name    = p_delta_history+"/" + p_output_c_inactivos_name   +"/"

def url_config_variables():

    # Variables Globales
    global v_url_apigee_omnicanal
    global v_url_conexion_invitado            
    global v_url_conexión_cliente_invitados   
    global v_url_conexión_cliente_registrados 
    global v_url_conexión_cliente_inactivos
    global v_content_Type_headers_conexion_invitado
    global v_authorization_headers_conexion_invitado
    global v_payload_conexion_invitado   
    
    # CONEXION A CADA UNA DE LAS APIS, DEV, QA, PROD
    
    # VALIDACIÓN de que exista la(s) variables en secret manager solicitada(s)
    #v_url_apigee_omnicanal             = "https://apidev.ecommerce.mobilityado.com/apis/"
    v_url_apigee_omnicanal = valid_content(os.environ.get('cf_omnicanal_url_apigee', 'NOT_CONFIGURED') ,    "cf_omnicanal_url_apigee")

    # VARIABLES EXTERNAS
    v_url_conexion_invitado            = v_url_apigee_omnicanal + p_url_invitado_name
    v_url_conexión_cliente_invitados   = v_url_apigee_omnicanal + p_url_clientes_invitados_name
    v_url_conexión_cliente_registrados = v_url_apigee_omnicanal + p_url_clientes_registrados_name  
    v_url_conexión_cliente_inactivos   = v_url_apigee_omnicanal + p_url_clientes_inactivos_name
    
    # SECRET MANAGER
    # CONFIGURACIÓN PROPORCIONADA DE LA API DEL SERVIDOR DE OMNICANAL POR JOSE AGUSTIN ORTEGA CAUDILLO - INTERNO DE ADO
    #v_payload_conexion_invitado               = 'grant_type=client_credentials&scope=ADOClientes&au='
    #v_content_Type_headers_conexion_invitado  = 'application/x-www-form-urlencoded'
    #v_authorization_headers_conexion_invitado = 'Basic YjAxYTQ0YTQ0YTAzNDRhNmI5Mjc5YjdhMmUwMGU5MGI6ZWJjOWExZWYtMTY1NS00OTQyLTk3NTctMDczNDAyMTJmYmVi'
    v_payload_conexion_invitado               = valid_content(os.environ.get('cf_omnicanal_payload_conexion_invitado', 'NOT_CONFIGURED') ,    "cf_omnicanal_payload_conexion_invitado")
    v_content_Type_headers_conexion_invitado  = valid_content(os.environ.get('cf_omnicanal_content_Type_headers_conexion_invitado', 'NOT_CONFIGURED') ,    "cf_omnicanal_content_Type_headers_conexion_invitado")
    v_authorization_headers_conexion_invitado = valid_content(os.environ.get('cf_omnicanal_authorization_headers_conexion_invitado', 'NOT_CONFIGURED') ,    "cf_omnicanal_authorization_headers_conexion_invitado")

def configuracion_bucket():

    # VARIABLE GLOBAL
    global bucket

    # Configuración del proyecto cliente
    storage_client = storage.Client(project= p_project_id)
    
    # Configuración del bucket cliente
    bucket = storage_client.get_bucket(p_landing_bucket_name)

# Metodo para validar cuando se ingresa una fecha automatica o manual
def obtener_fecha_automatica(p_fecha_consultar):    
    
    # NOTA: LA FECHA DE OMNICANAL REQUIERE EL FORMATO DD-MM-YYYY
    # NOTA: EL FORMATO DE FECHA VIENE EN STRING EN FORMATO DD-MM-YYYY

    # EJECUCIÓN AUTOMATICA - Cloud Scheduler
    # No hay un valor asignado a la variable de fecha a consultar
    if ((p_fecha_consultar == "NOT_CONFIGURED")):
        # Asignar fecha de ayer
        v_start_date = date.today() - timedelta(days = 1)

    # EJECUCION MANUAL - PARAMETRIZADO
    else:
        #Convertir a formato Date YYYY-mm-DD
        v_start_date = datetime.strptime(p_fecha_consultar, '%d-%m-%Y')
        print(type(v_start_date))
        print(v_start_date)  # printed in default format
       
    #AMBOS CASOS SERAN DE DATE YYYY-MM-DD

    # Dividir en año, mes fecha ini, fecha fin en STRING
    v_start_year  = str(v_start_date.year)
    #Checar casos donde le falta agregar un 0
    v_start_month = str(v_start_date.month).zfill(2)
    v_start_day   = str(v_start_date.day).zfill(2)

    # CONCATENAR FECHA
    fecha_ejecucion = str(v_start_day) + "-" + str(v_start_month) + "-" + str(v_start_year)
    return fecha_ejecucion,v_start_year,v_start_month,v_start_day

    ####################################################################################

# CONFIGURACIÓN TOKEN PARA INVITADO A LAS API'S, DURA 1 HORA
# NOTA: ESTE METODO ES EJECUTADO 3 VECES, DEBIDO A QUE EL SERVIDOR CONSIDERA A CADA API COMO UN USUARIO LOGEADO
#       Y ESTA DEBE ACTUALIZAR LA CONEXIÓN CADA QUE SE CONSULTA UNA API DISTINTA, DE OTRA MANERA GENERA ERROR
def conexion_invitado():

    url = v_url_conexion_invitado
    payload = v_payload_conexion_invitado
    headers = {
      'Content-Type': v_content_Type_headers_conexion_invitado,
      'Authorization': v_authorization_headers_conexion_invitado
    }
    # Meter un validador por tiempo excedido
    response = requests.request("POST", url, headers=headers, data=payload,verify=False)

    #Validar que exista información en la conexión, en dado caso que no hay información, no continuar el request para esa API
    if response.status_code != 200:
        print('error: ' + str(response.status_code))
        access_token = "NO HAY CONEXION AL SERVIDOR INVITADO"
    else:
        #print('Success connection to Omnicanal Invitado')
        response_json = response.json()
        access_token = response_json["access_token"]

    return access_token
    
# CONFIGURACIÓN INGESTA API Clientes invitados
def obtener_clientes_invitados(v_fecha_consultar):

    access_token = conexion_invitado()
    authorization = 'Bearer ' + access_token
    url = v_url_conexión_cliente_invitados
    
    payload = json.dumps({
      "fechaConsultar": v_fecha_consultar #"09-10-2019"
    })
    
    headers = {
      'Authorization': authorization ,
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload, verify= False)

    if response.status_code != 200:
        print('error: ' + str(response.status_code))
    else:
        print('Success connection to Omnicanal API ObtenerClientesInvitados')

    response_json = response.json()

    # VALIDAR QUE LA CONEXIÓN SE EJECUTE Y CONTENGA DATOS
    if response_json["encabezado"]["code"]==100:
        print("status: " + response_json["encabezado"]["status"])
        # Validar que el key sea o contendio o encabezado, code 101
        response_contenido_cliente = response_json["contenido"]["clientes"][0]
    else:
        print("status: " + response_json["encabezado"]["status"])
        response_contenido_cliente = "CONEXION OK - PERO NO HAY DATOS"
    # Regresa la salida en formato JSON
    return response_contenido_cliente

def obtener_clientes_registrados(v_fecha_consultar):
    
    url = "https://apidev.ecommerce.mobilityado.com/apis/1.1.0/clientesOmnicanal/1.0.0/obtenerClientesRegistrados"
    access_token = conexion_invitado()
    authorization = 'Bearer ' + access_token

    payload = json.dumps({
      "fechaConsultar": v_fecha_consultar
    })
    headers = {
     'Authorization': authorization ,
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code != 200:
        print('error: ' + str(response.status_code))
    else:
        print('Success connection to Omnicanal API ObtenerClientesRegistrados')

    response_json = response.json()

    # VALIDAR QUE LA CONEXIÓN SE EJECUTE Y CONTENGA DATOS
    if response_json["encabezado"]["code"]==100:
        print("status: " + response_json["encabezado"]["status"])
        # Validar que el key sea o contendio o encabezado, code 101
        response_json

    else:
        print("status: " + response_json["encabezado"]["status"])
        response_json = "CONEXION OK - PERO NO HAY DATOS"

    # Regresa la salida en formato JSON
    return response_json

    #df = pd.DataFrame([response_contenido_cliente])
    #print(df)

def obtener_clientes_inactivos(v_fecha_consultar):
    
    url = "https://apidev.ecommerce.mobilityado.com/apis/1.1.0/clientesOmnicanal/1.0.0/obtenerClientesInactivos"
    access_token = conexion_invitado()
    authorization = 'Bearer ' + access_token

    payload = json.dumps({
      "fechaConsultar": v_fecha_consultar
    })
    headers = {
     'Authorization': authorization ,
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code != 200:
        print('error: ' + str(response.status_code))
    else:
        print('Success connection to Omnicanal API ObtenerClientesInactivos')

    response_json = response.json()

    # VALIDAR QUE LA CONEXIÓN SE EJECUTE Y CONTENGA DATOS
    if response_json["encabezado"]["code"]==100:
        print("status: " + response_json["encabezado"]["status"])
        # Validar que el key sea o contendio o encabezado, code 101
        response_contenido_cliente = response_json["contenido"]["clientes"][0]
    else:
        print("status: " + response_json["encabezado"]["status"])
        response_contenido_cliente = "CONEXION OK - PERO NO HAY DATOS"    
    
    return response_contenido_cliente

    #df = pd.DataFrame([response_contenido_cliente])
    #print(df)

def escribir_gcs(v_nombre_api, v_json, v_output_path_name,v_start_year,v_start_month,v_start_day):
    #v_output_json = "{'clienteId': '475', 'correoElectronico': 'luis.apresa@rjconsultores.com.mx', 'fecHorCreacion': '07-03-2016 11:57:16', 'fecHorAct': '09-10-2019 19:01:20', 'idClienteOriginal': '973', 'cantVisitas': '0'}"
    df = pd.DataFrame([v_json])
    #print(df)

    #v_start_year  = "2024"
    #v_start_month = "10"
    #v_start_day   = "31"

    # Escribir en un bucket en PARQUET
    try:
        bucket.blob('{}{}{}{}/omnicanal_{}{}{}.parquet'.format(v_output_path_name, v_start_year,v_start_month,v_start_day, v_start_year,v_start_month,v_start_day))\
              .upload_from_string(df.to_parquet(),'application/octet-stream')
    except AttributeError:
        logger.log_text('Error al guardar info en cloud storage', severity=severity.INFO)      

    ## Escribir en un bucket en CSV
    #try:
    #    bucket.blob('{}zendesk_{}{}{}.csv'.format(v_output_path_name,str(v_start_year),str(v_start_month),str(v_start_day)))\
    #          .upload_from_string(df.to_csv(), 'text/csv')

    #except AttributeError:
    #    logger.log_text('Error al guardar info en cloud storage', severity=severity.INFO) 

    print("Escribiendo " + v_nombre_api + " .....")

# Función principal de la Cloud Functions
def main(request):
    
    # Método con configuración para generar logs
    configuracion_log_cliente()

    #Método con parametros externos - VARIABLES GLOBALES 
    parametros_externos()

    # Método con todas la URLs  - VARIABLES GLOBALES
    url_config_variables()

    # Método para obtener fecha automaticas - VARIABLES GLOBALES
    v_fecha_consultar,v_start_year,v_start_month,v_start_day = obtener_fecha_automatica(p_fecha_consultar)
    print("Fecha a consultar:  "+ v_fecha_consultar)
    print("Fecha a consultar:  "+ v_start_year)
    print("Fecha a consultar:  "+ v_start_month)
    print("Fecha a consultar:  "+ v_start_day)

    # Método para obtener la configuración de los buckets y logging - VARIABLES GLOBALES
    configuracion_bucket()

    # Asignar a una variable el metodo de conexión al servidor invitado para que sea ejecutado solo 1 vez
    validacion_conexion_invitado_ok = conexion_invitado()

    # Validar que si exista acceso al servidor invitado
    if validacion_conexion_invitado_ok != "NO HAY CONEXION AL SERVIDOR INVITADO":     

        # obtener_clientes_invitados API
        v_obtener_clientes_invitados = obtener_clientes_invitados(v_fecha_consultar)
        if v_obtener_clientes_invitados != "CONEXION OK - PERO NO HAY DATOS":
            escribir_gcs("obtener clientes invitados",v_obtener_clientes_invitados, p_output_c_invitado_path_name,v_start_year,v_start_month,v_start_day)

        # obtener_clientes_registrados API
        v_obtener_clientes_registrados = obtener_clientes_registrados(v_fecha_consultar)
        if v_obtener_clientes_registrados != "CONEXION OK - PERO NO HAY DATOS":
            escribir_gcs("obtener clientes registrados",v_obtener_clientes_registrados, p_output_c_registrado_path_name,v_start_year,v_start_month,v_start_day)

        # obtener_clientes_inactivos API
        v_obtener_clientes_inactivos = obtener_clientes_inactivos(v_fecha_consultar)
        if v_obtener_clientes_inactivos != "CONEXION OK - PERO NO HAY DATOS":
            escribir_gcs("obtener clientes inactivos",v_obtener_clientes_inactivos, p_output_c_inactivo_path_name,v_start_year,v_start_month,v_start_day)
    else:
        print(validacion_conexion_invitado_ok)


if __name__ == "__main__":
    main(None)