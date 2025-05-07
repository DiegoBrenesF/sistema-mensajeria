# Sistema de Mensajería con Broker

## Descripción General

Este proyecto implementa un sistema de mensajería basado en el patrón arquitectónico publicador/suscriptor con un broker como intermediario. El sistema permite la comunicación asíncrona entre productores (que generan mensajes) y consumidores (que procesan estos mensajes) a través de una cola gestionada por un servidor central (broker).

## Componentes Principales

El sistema consta de tres componentes fundamentales:

- **Broker**: Actúa como intermediario central que gestiona la cola de mensajes, las conexiones de clientes y la distribución de mensajes.
- **Productor**: Genera mensajes y los envía al broker para su distribución.
- **Consumidor**: Se conecta al broker para recibir y procesar los mensajes disponibles.

## Características del Sistema

- **Modelo Publicador/Suscriptor**: Desacoplamiento entre productores y consumidores de mensajes.
- **Comunicación TCP/IP**: Permite conexiones de clientes remotos.
- **Memoria Compartida**: Utiliza IPC (Comunicación Entre Procesos) de UNIX para una cola de mensajes eficiente.
- **Pool de Hilos**: Manejo concurrente de múltiples clientes y tareas.
- **Persistencia**: Almacenamiento de mensajes en disco para recuperación en caso de fallos.
- **Gestión de Sesiones**: Seguimiento de clientes y su actividad.
- **Detección de Inactividad**: Desconexión automática de clientes inactivos.
- **Manejo de Señales**: Finalización ordenada ante señales del sistema.
- **Reconexión Automática**: Los clientes pueden reconectarse en caso de pérdida de conexión.

## Estructura del Proyecto

```
├── bin/            # Directorio para archivos ejecutables
├── src/            # Código fuente
│   ├── broker.c    # Implementación del broker
│   ├── producer.c  # Implementación del productor
│   └── consumer.c  # Implementación del consumidor
└── Makefile        # Archivo de configuración para compilación
```

## Requisitos del Sistema

- Sistema operativo compatible con UNIX/Linux
- Compilador GCC
- Bibliotecas estándar de C
- Bibliotecas POSIX Threads (pthread)

## Compilación

### Compilación de todos los componentes

Para compilar todos los componentes del sistema:

```bash
make
```

Este comando creará el directorio `bin/` si no existe y compilará los tres ejecutables.

### Compilación individual de componentes

Para compilar cada componente individualmente:

**Broker:**
```bash
gcc -o bin/broker src/broker.c -lpthread -Wall
```

**Productor:**
```bash
gcc -o bin/producer src/producer.c -lpthread -Wall
```

**Consumidor:**
```bash
gcc -o bin/consumer src/consumer.c -Wall
```

## Ejecución

Es importante iniciar los componentes en el siguiente orden:

1. Primero el Broker (para recibir conexiones)
2. Luego los Productores y Consumidores (en cualquier orden)

### Broker

```bash
./bin/broker <archivo_persistencia>
```

Donde:
- `<archivo_persistencia>`: Ruta al archivo donde se guardarán los mensajes persistentes

Ejemplo:
```bash
./bin/broker mensajes.dat
```

### Productor

**Modo Interactivo:**
```bash
./bin/producer
```
En este modo, el programa le solicitará ingresar mensajes. Escriba el mensaje y presione Enter para enviarlo. Para salir, ingrese `q`.

**Modo Automático:**
```bash
./bin/producer -n [número_de_mensajes] -i [intervalo_ms]
```

Donde:
- `-n`: Número de mensajes a enviar
- `-i`: Intervalo entre mensajes en milisegundos (opcional, valor predeterminado: 1000 ms)

**Ayuda:**
```bash
./bin/producer -h
```

### Consumidor

```bash
./bin/consumer
```

## Configuración

La configuración de cada componente se realiza mediante constantes definidas en los archivos fuente:

### Broker
- Puerto de escucha: 8080
- Tamaño máximo de la cola: 1000 mensajes
- Tamaño máximo por mensaje: 1024 bytes
- Máximo de clientes simultáneos: 100

### Productor
- `BROKER_HOST`: Hostname o dirección IP del broker (predeterminado: "localhost")
- `BROKER_PORT`: Puerto en el que escucha el broker (predeterminado: 8080)
- `BUFFER_SIZE`: Tamaño del buffer para mensajes (predeterminado: 2048 bytes)
- `MAX_MSG_SIZE`: Tamaño máximo de un mensaje (predeterminado: 1024 bytes)

### Consumidor
- `SERVER_IP`: Dirección IP del servidor broker (predeterminado: "127.0.0.1")
- `SERVER_PORT`: Puerto del broker (predeterminado: 8080)
- `BUFFER_SIZE`: Tamaño del buffer para mensajes (predeterminado: 1024 bytes)

## Protocolo de Comunicación

El sistema utiliza un protocolo simple basado en texto para la comunicación entre componentes:

### Registro de Clientes

- **Productor**: `REGISTER:PRODUCER`
- **Consumidor**: `REGISTER:CONSUMER` o `REGISTER:CONSUMER:<id>`
- **Respuesta**: `OK:PRODUCER` o `OK:CONSUMER:<id>`

### Envío de Mensajes (Productor)

- **Enviar**: `SEND:<contenido>`
- **Respuesta**: `OK:MSG_ID:<id>` o `ERROR:<mensaje>`
- **Heartbeat**: `HEARTBEAT:timestamp`

### Recepción de Mensajes (Consumidor)

- **Solicitar**: `GET`
- **Respuesta**: Contenido del mensaje o `ERROR:No messages available`

### Notificaciones

- **Nuevos mensajes**: `NOTIFY:MESSAGES_AVAILABLE:<count>`

## Arquitectura del Sistema

### Componentes del Broker

1. **Cola de Mensajes**
   - Implementada en memoria compartida
   - Controlada por semáforos para sincronización
   - Estructura circular para almacenamiento eficiente

2. **Servidor de Red**
   - Acepta conexiones TCP en el puerto 8080
   - Gestiona múltiples clientes concurrentemente

3. **Pool de Hilos**
   - Procesamiento paralelo de solicitudes
   - Distribución de carga entre hilos trabajadores

4. **Sistema de Persistencia**
   - Guarda mensajes en disco
   - Mantiene offsets de consumidores

### Recursos IPC Utilizados

El sistema utiliza los siguientes recursos IPC:
- Memoria compartida (clave: 0x1234)
- Conjunto de semáforos (clave: 0x5678)

### Archivos del Sistema

- **Archivo de Persistencia**: Almacena mensajes en formato `MSG_ID:TIMESTAMP:SIZE:CONTENT`
- **Archivo de Offsets**: Guarda posiciones de consumidores en formato `CONSUMER:ID:OFFSET`
- **Archivo de Log**: Registra operaciones de la cola en `cola_mensajes.log`

## Manejo de Errores

El sistema implementa un manejo robusto de errores:

- Validación de parámetros de entrada
- Detección de desconexiones de clientes
- Recuperación ante fallos temporales
- Reconexión automática de clientes
- Límite de errores consecutivos para evitar ciclos infinitos
- Verificaciones para errores de socket, conexión y comunicación

## Flujo de Trabajo Típico

1. Iniciar el broker proporcionando un archivo de persistencia.
2. Iniciar uno o más consumidores para suscribirse a los mensajes.
3. Iniciar uno o más productores para generar mensajes.
4. Los productores envían mensajes al broker.
5. El broker notifica a los consumidores sobre mensajes disponibles.
6. Los consumidores solicitan y procesan los mensajes.

## Consideraciones de Seguridad

- Limpieza de recursos IPC huérfanos al inicio
- Timeouts en operaciones para evitar bloqueos
- Detección y manejo de clientes inactivos
- Finalización ordenada ante señales del sistema

## Limitaciones

- No implementa mecanismos de seguridad como TLS/SSL
- No permite la autenticación de clientes
- Tamaño máximo de la cola: 1000 mensajes
- Tamaño máximo por mensaje: 1024 bytes
- Máximo de clientes simultáneos: 100

## Contribuciones

Para contribuir a este proyecto:
1. Fork del repositorio
2. Cree una rama para su funcionalidad (`git checkout -b feature/nueva-funcionalidad`)
3. Haga commit de sus cambios (`git commit -am 'Añadir nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Cree un nuevo Pull Request

## Limpieza

Para eliminar todos los archivos generados durante la compilación:

```bash
make clean
```
