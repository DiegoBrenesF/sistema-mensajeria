#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdbool.h>

/* Configuración del cliente */
#define BROKER_HOST "localhost"  // Hostname o IP del broker
#define BROKER_PORT 8080         // Puerto del broker
#define BUFFER_SIZE 2048         // Tamaño del buffer para mensajes
#define MAX_MSG_SIZE 1024        // Tamaño máximo de un mensaje

/* Estructura para el productor */
typedef struct {
    int socket;                  // Socket de conexión con el broker
    bool connected;              // Indica si está conectado al broker
    bool running;                // Indica si el productor está en ejecución
    int message_count;           // Contador de mensajes enviados
    pthread_t heartbeat_thread;  // Hilo para mantener la conexión activa
    pthread_mutex_t socket_mutex; // Mutex para proteger el acceso al socket
} producer_t;

/* Variable global */
producer_t producer;

/**
 * Manejador de señales para finalización ordenada
 */
void signal_handler(int sig) {
    printf("\nRecibida señal %d. Finalizando productor...\n", sig);
    producer.running = false;
}

/**
 * Configura los manejadores de señales
 */
void setup_signal_handlers() {
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    
    sigaction(SIGINT, &sa, NULL);   // Ctrl+C
    sigaction(SIGTERM, &sa, NULL);  // kill
}

/**
 * Inicializa el productor
 */
void initialize_producer() {
    memset(&producer, 0, sizeof(producer));
    producer.socket = -1;
    producer.connected = false;
    producer.running = true;
    producer.message_count = 0;
    
    pthread_mutex_init(&producer.socket_mutex, NULL);
    
    // Inicializar generador de números aleatorios
    srand(time(NULL));
}

/**
 * Limpia los recursos utilizados por el productor
 */
void cleanup_producer() {
    pthread_mutex_lock(&producer.socket_mutex);
    
    if (producer.socket != -1) {
        close(producer.socket);
        producer.socket = -1;
    }
    producer.connected = false;
    
    pthread_mutex_unlock(&producer.socket_mutex);
    
    pthread_mutex_destroy(&producer.socket_mutex);
    
    printf("Productor finalizado correctamente\n");
}

/**
 * Conecta el productor al broker
 * @return true si la conexión fue exitosa, false en caso contrario
 */
bool connect_to_broker() {
    struct sockaddr_in server_addr;
    struct hostent *server;
    char buffer[BUFFER_SIZE];
    int socket_fd;
    
    // Crear socket
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        perror("Error creando socket");
        return false;
    }
    
    // Obtener dirección del servidor
    server = gethostbyname(BROKER_HOST);
    if (server == NULL) {
        fprintf(stderr, "Error: No se encuentra el host %s\n", BROKER_HOST);
        close(socket_fd);
        return false;
    }
    
    // Configurar dirección del servidor
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    memcpy(&server_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    server_addr.sin_port = htons(BROKER_PORT);
    
    // Conectar al servidor
    if (connect(socket_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Error conectando al broker");
        close(socket_fd);
        return false;
    }
    
    // Establecer socket como no bloqueante
    int flags = fcntl(socket_fd, F_GETFL, 0);
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
    
    // Registrarse como productor
    strcpy(buffer, "REGISTER:PRODUCER");
    
    int bytes_sent = send(socket_fd, buffer, strlen(buffer), 0);
    if (bytes_sent < 0) {
        perror("Error enviando mensaje de registro");
        close(socket_fd);
        return false;
    }
    
    // Esperar respuesta del broker
    fd_set read_fds;
    struct timeval tv;
    int ready;
    
    FD_ZERO(&read_fds);
    FD_SET(socket_fd, &read_fds);
    
    // Timeout de 5 segundos para respuesta
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    
    ready = select(socket_fd + 1, &read_fds, NULL, NULL, &tv);
    
    if (ready <= 0) {
        if (ready == 0) {
            fprintf(stderr, "Timeout esperando respuesta del broker\n");
        } else {
            perror("Error en select");
        }
        close(socket_fd);
        return false;
    }
    
    // Leer respuesta
    int bytes_received = recv(socket_fd, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received <= 0) {
        perror("Error recibiendo respuesta del broker");
        close(socket_fd);
        return false;
    }
    
    buffer[bytes_received] = '\0';
    
    // Verificar respuesta
    if (strncmp(buffer, "OK:PRODUCER", 11) != 0) {
        fprintf(stderr, "Error en respuesta del broker: %s\n", buffer);
        close(socket_fd);
        return false;
    }
    
    // Actualizar estado del productor
    pthread_mutex_lock(&producer.socket_mutex);
    
    producer.socket = socket_fd;
    producer.connected = true;
    
    pthread_mutex_unlock(&producer.socket_mutex);
    
    printf("Conectado al broker exitosamente como productor\n");
    return true;
}

/**
 * Envía un mensaje al broker
 * @param message Mensaje a enviar
 * @return ID del mensaje asignado por el broker o -1 en caso de error
 */
int send_message(const char *message) {
    char buffer[BUFFER_SIZE];
    char send_buffer[BUFFER_SIZE];
    int msg_id = -1;
    bool retry = true;
    
    if (!message || strlen(message) == 0 || strlen(message) > MAX_MSG_SIZE) {
        fprintf(stderr, "Error: Mensaje inválido\n");
        return -1;
    }
    
    pthread_mutex_lock(&producer.socket_mutex);
    
    // Verificar conexión
    if (!producer.connected) {
        fprintf(stderr, "Error: No hay conexión con el broker\n");
        pthread_mutex_unlock(&producer.socket_mutex);
        return -1;
    }
    
    // Preparar mensaje
    snprintf(send_buffer, BUFFER_SIZE, "SEND:%s", message);
    
    // Enviar mensaje
    int bytes_sent = send(producer.socket, send_buffer, strlen(send_buffer), 0);
    if (bytes_sent < 0) {
        perror("Error enviando mensaje");
        producer.connected = false;
        close(producer.socket);
        producer.socket = -1;
        pthread_mutex_unlock(&producer.socket_mutex);
        return -1;
    }
    
    // Esperar respuesta
    fd_set read_fds;
    struct timeval tv;
    int ready;
    
    while (retry && producer.running) {
        FD_ZERO(&read_fds);
        FD_SET(producer.socket, &read_fds);
        
        // Timeout de 2 segundos para respuesta
        tv.tv_sec = 2;
        tv.tv_usec = 0;
        
        ready = select(producer.socket + 1, &read_fds, NULL, NULL, &tv);
        
        if (ready < 0) {
            if (errno == EINTR) {
                // Interrumpido por señal, reintentar
                continue;
            }
            perror("Error en select");
            producer.connected = false;
            close(producer.socket);
            producer.socket = -1;
            pthread_mutex_unlock(&producer.socket_mutex);
            return -1;
        } else if (ready == 0) {
            // Timeout, reintentar una vez más
            if (retry) {
                retry = false;
                continue;
            }
            fprintf(stderr, "Timeout esperando respuesta del broker\n");
            pthread_mutex_unlock(&producer.socket_mutex);
            return -1;
        }
        
        // Leer respuesta
        int bytes_received = recv(producer.socket, buffer, BUFFER_SIZE - 1, 0);
        
        if (bytes_received <= 0) {
            if (bytes_received == 0) {
                fprintf(stderr, "Conexión cerrada por el broker\n");
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No hay datos disponibles, continuar
                    continue;
                }
                perror("Error recibiendo respuesta");
            }
            producer.connected = false;
            close(producer.socket);
            producer.socket = -1;
            pthread_mutex_unlock(&producer.socket_mutex);
            return -1;
        }
        
        buffer[bytes_received] = '\0';
        
        // Procesar respuesta
        if (strncmp(buffer, "OK:MSG_ID:", 10) == 0) {
            msg_id = atoi(buffer + 10);
            printf("Mensaje enviado correctamente, ID: %d\n", msg_id);
            producer.message_count++;
            retry = false;
        } else {
            fprintf(stderr, "Error en respuesta del broker: %s\n", buffer);
            retry = false;
        }
    }
    
    pthread_mutex_unlock(&producer.socket_mutex);
    return msg_id;
}

/**
 * Función para el hilo de heartbeat
 * Mantiene la conexión activa enviando mensajes periódicos
 */
void *heartbeat_thread_func(void *arg) {
    char heartbeat_msg[50];
    
    while (producer.running) {
        // Cada 60 segundos, enviar un heartbeat
        sleep(60);
        
        if (!producer.connected || !producer.running) {
            continue;
        }
        
        // Crear mensaje de heartbeat
        snprintf(heartbeat_msg, sizeof(heartbeat_msg), "HEARTBEAT:%d", (int)time(NULL));
        
        // Intentar enviar el heartbeat
        pthread_mutex_lock(&producer.socket_mutex);
        
        if (producer.connected) {
            if (send(producer.socket, heartbeat_msg, strlen(heartbeat_msg), 0) < 0) {
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    perror("Error enviando heartbeat");
                    producer.connected = false;
                    close(producer.socket);
                    producer.socket = -1;
                }
            }
        }
        
        pthread_mutex_unlock(&producer.socket_mutex);
    }
    
    return NULL;
}

/**
 * Reconecta al broker si la conexión se pierde
 */
void reconnect_if_needed() {
    pthread_mutex_lock(&producer.socket_mutex);
    
    bool needs_reconnect = !producer.connected;
    
    pthread_mutex_unlock(&producer.socket_mutex);
    
    if (needs_reconnect) {
        printf("Intentando reconectar al broker...\n");
        
        // Esperar un poco antes de reconectar
        sleep(2);
        
        if (connect_to_broker()) {
            printf("Reconexión exitosa\n");
        } else {
            printf("No se pudo reconectar\n");
        }
    }
}

/**
 * Genera un mensaje de ejemplo
 * @param buffer Buffer donde se almacenará el mensaje
 * @param size Tamaño del buffer
 */
void generate_sample_message(char *buffer, size_t size) {
    static const char *topics[] = {
        "sistema", "red", "usuario", "aplicacion", "seguridad"
    };
    static const char *severities[] = {
        "INFO", "WARNING", "ERROR", "CRITICAL", "DEBUG"
    };
    static const char *messages[] = {
        "Operación completada con éxito",
        "Se ha detectado un posible problema",
        "No se pudo completar la operación",
        "Falla crítica del sistema",
        "Conexión establecida",
        "Conexión cerrada",
        "Autenticación fallida",
        "Nuevo usuario registrado",
        "Recursos bajos",
        "Actualización disponible"
    };
    
    // Seleccionar aleatoriamente un tema, severidad y mensaje
    const char *topic = topics[rand() % (sizeof(topics) / sizeof(topics[0]))];
    const char *severity = severities[rand() % (sizeof(severities) / sizeof(severities[0]))];
    const char *message = messages[rand() % (sizeof(messages) / sizeof(messages[0]))];
    
    // Generar un ID aleatorio para este evento
    int event_id = rand() % 10000;
    
    // Formatear el mensaje como JSON
    snprintf(buffer, size,
             "{"
             "\"timestamp\":\"%ld\","
             "\"event_id\":%d,"
             "\"topic\":\"%s\","
             "\"severity\":\"%s\","
             "\"message\":\"%s\","
             "\"producer_id\":%d"
             "}",
             time(NULL), event_id, topic, severity, message, getpid());
}

/**
 * Función principal
 */
int main(int argc, char *argv[]) {
    int num_messages = 0;
    int interval_ms = 1000;  // Intervalo predeterminado: 1 segundo
    bool interactive_mode = true;
    
    // Procesar argumentos de línea de comandos
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
            num_messages = atoi(argv[i + 1]);
            interactive_mode = false;
            i++;
        } else if (strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
            interval_ms = atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("Uso: %s [opciones]\n", argv[0]);
            printf("Opciones:\n");
            printf("  -n N        Enviar N mensajes y salir\n");
            printf("  -i MS       Intervalo entre mensajes en milisegundos (predeterminado: 1000)\n");
            printf("  -h, --help  Mostrar esta ayuda\n");
            return EXIT_SUCCESS;
        }
    }
    
    // Inicializar productor
    initialize_producer();
    
    // Configurar manejadores de señales
    setup_signal_handlers();
    
    // Conectar al broker
    if (!connect_to_broker()) {
        fprintf(stderr, "No se pudo conectar al broker. Saliendo.\n");
        cleanup_producer();
        return EXIT_FAILURE;
    }
    
    // Iniciar hilo de heartbeat
    if (pthread_create(&producer.heartbeat_thread, NULL, heartbeat_thread_func, NULL) != 0) {
        perror("Error creando hilo de heartbeat");
        cleanup_producer();
        return EXIT_FAILURE;
    }
    
    char message_buffer[MAX_MSG_SIZE];
    
    if (interactive_mode) {
        printf("\n=== Productor de Mensajes ===\n");
        printf("Escriba mensajes para enviar al broker o 'q' para salir\n");
        
        while (producer.running) {
            printf("\nMensaje> ");
            if (fgets(message_buffer, MAX_MSG_SIZE, stdin) == NULL) {
                break;
            }
            
            // Eliminar salto de línea
            size_t len = strlen(message_buffer);
            if (len > 0 && message_buffer[len - 1] == '\n') {
                message_buffer[len - 1] = '\0';
            }
            
            // Salir si se ingresa 'q'
            if (strcmp(message_buffer, "q") == 0) {
                break;
            }
            
            // Reconectar si es necesario
            reconnect_if_needed();
            
            // Enviar mensaje si hay algo que enviar
            if (strlen(message_buffer) > 0) {
                int msg_id = send_message(message_buffer);
                if (msg_id >= 0) {
                    printf("Mensaje enviado con ID: %d\n", msg_id);
                }
            }
        }
    } else {
        // Modo automático: enviar número específico de mensajes
        printf("Enviando %d mensajes con intervalo de %d ms\n", num_messages, interval_ms);
        
        for (int i = 0; i < num_messages && producer.running; i++) {
            // Generar mensaje de ejemplo
            generate_sample_message(message_buffer, MAX_MSG_SIZE);
            
            // Reconectar si es necesario
            reconnect_if_needed();
            
            // Enviar mensaje
            int msg_id = send_message(message_buffer);
            if (msg_id >= 0) {
                printf("Mensaje %d/%d enviado con ID: %d\n", i + 1, num_messages, msg_id);
            } else {
                printf("Error enviando mensaje %d/%d\n", i + 1, num_messages);
            }
            
            // Esperar el intervalo especificado
            usleep(interval_ms * 1000);  // convertir ms a microsegundos
        }
    }
    
    // Terminar hilo de heartbeat
    producer.running = false;
    pthread_join(producer.heartbeat_thread, NULL);
    
    // Limpiar recursos
    cleanup_producer();
    
    return EXIT_SUCCESS;
}
