# Coderhouse - Pre-entrega 2
Este proyecto es parte de una serie de entregas para el curso de Coder House. La presente versión corresponde a la pre-entrega 2, donde se construye a partir de la pre-entrega 1, sobre la consulta de la API elegida, para en esta instancia lograr la carga de datos a Redshift.

## COMENTARIO IMPORTANTE
Lamentablemente, no logré insertar los datos en la base de redshift. El error recibido es "'Connection' object has no attribute 'cursor'".
Estuve varios días intentando resolverlo pero agoté mis alternativas. Volví a ver todoas las clases y ejecuté todo tal cual se podía ver. Conexión a redshift de distintas formas, distintas funciones para la lectura de credenciales, to_sql dentro y fuera de la función, etc. 
Por lo que pude conversar, varios compañer@s tuvieron el mismo inconveniente. Me comentaron que algunos lo resolvieron ejecutando el mismo código en Google Colabs, pero estuve intentando hacerlo y al no haber usando nunca esta plataforma, no tuve tiempo suficiente para hacer lo mismo. 

En principio pude ver que el error que arroja informa que no se está usando sqlalchemy, y la sección del código de python desde donde se envía el error, parece ser la función interna de python desde donde se intenta hacer una conexión con sqlite. Pero no logré identificar por qué. 

Si se requiere más info para entender el error, quedo atento para enviarla. 

### API BCRA
La API elegida es la del Banco central de la República Argentina. 
Link de la documentación de la API: https://estadisticasbcra.com/api/documentacion 

En este proyecto se consultan los siguientes endpoints: 
* "/plazo_fijo": monto en plazos fijos expresado en miles.
* "/depositos": monto en depósitos expresado en miles.
* "/cajas_ahorro": monto en cajas de ahorro expresado en miles.
* "/cuentas_corrientes": monto en cuentas corrientes expresado en miles.
* "/usd": cotización del dólar blue.
* "/usd_of": cotización del dólar oficial.

## Descripción del Proyecto
`bcra-consolidate.py` Es script principal de este repositorio. Interactúa con la API del BCRA para obtener información que luego es procesada y convertida a un dataframe. Luego conecta a Redshift e intenta hacer la inserción de datos, controlando duplicados a través de una tabla transitoria/staging "stg_bcra". 

`utils.py` Es el archivo de funciones que script principal utiliza para la lectura de credenciales de la API, construcción del _conn_string_ y conexión a Redshift + carga de datos en la base de datos.

`config.ini` Es el archivo de credenciales que consulta utils.py para autenticación y datos de conexión. A continuación se detalla la estructura. 

### Estructura del archivo `config.ini`
El archivo `config.ini` debe tener la siguiente estructura:

[api_bcra]
token = TU_TOKEN_AQUI

[redshift]
host = TU_HOST_AQUI
dbname = TU_DBNAME_AQUI
user = TU_USER_AQUI
password = TU_PASSWORD_AQUI
port = TU_PORT_AQUI

El archivo example-config.ini facilita la creación, únicamente solicitando la inserción de credenciales y datos propios.

### Instalación de Dependencias
Antes de ejecutar el script, es necesario instalar las librerías listadas en el archivo `requirements.txt` con el comando:

`pip install -r requirements.txt`

### Ejecución del Script Principal
Una vez configurado el entorno, ya se puede ejecutar el script principal con el comando:

`python bcra-consolidate.py`

## Comentarios y Buenas Prácticas

Actualmente trabajo en un equipo de infraestructura, con lo cual no tengo relación diaria con estas herramientas. Cualquier feedback relacionado con las buenas prácticas en compartir y documentar proyectos de este tipo será muy bienvenida.
