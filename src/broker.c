#define _GNU_SOURCE  // Para pthread_timedjoin_np
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <sys/select.h>
#include <time.h>
#include <poll.h>

#include <sys/ipc.h> /* Bibliotecas para cola de mensajes */
#include <sys/shm.h> /* Bibliotecas para cola de mensajes */
#include <sys/sem.h> /* Bibliotecas para cola de mensajes */
#include <sys/stat.h> /* Bibliotecas para cola de mensajes */

#define SHM_KEY 0x1234       /* Clave para la memoria compartida */ /* Cola de mensajes */
#define SEM_KEY 0x5678       /* Clave para el conjunto de semáforos */ /* Cola de mensajes */
#define MAX_QUEUE_SIZE 1000  /* Tamaño máximo de la cola de mensajes */ /* Cola de mensajes */
#define MAX_MSG_SIZE 1024    /* Tamaño máximo de un mensaje individual */ /* Cola de mensajes */
#define LOG_FILE "cola_mensajes.log"  /* Archivo de registro */ /* Cola de mensajes */

/* Índices de semáforos */ /* Cola de mensajes */
#define SEM_MUTEX 0    /* Para exclusión mutua en la cola */ /* Cola de mensajes */
#define SEM_EMPTY 1    /* Cuenta espacios vacíos disponibles */ /* Cola de mensajes */
#define SEM_FULL 2     /* Cuenta mensajes disponibles para consumir */ /* Cola de mensajes */
#define SEM_COUNT 3    /* Total de semáforos necesarios */ /* Cola de mensajes */

/* Configuración del servidor */
#define PORT 8080
#define MAX_CLIENTS 100
#define BUFFER_SIZE 2048
#define MAX_GROUP_NAME 64

/* Tipos de clientes */
#define CLIENT_TYPE_PRODUCER 1
#define CLIENT_TYPE_CONSUMER 2

/* Tipos de tareas para el pool de hilos */
#define TASK_CLIENT_HANDLER 1
#define TASK_DISTRIBUTOR    2
#define TASK_PERSISTER      3
#define TASK_INACTIVE_CHECK 4

/* Definiciones para el pool de hilos */
#define THREAD_POOL_SIZE 10  /* Tamaño del pool de hilos */

/* Estructura para un mensaje individual */ /* Cola de mensajes */
typedef struct {
    int msg_id;            /* ID único del mensaje */
    time_t timestamp;      /* Momento de recepción */
    size_t size;           /* Tamaño del contenido */
    char content[MAX_MSG_SIZE]; /* Contenido del mensaje */
} message_t; /* Cola de mensajes */

/* Estructura de la cola en memoria compartida */ /* Cola de mensajes */
typedef struct {
    int head;              /* Índice para extraer mensajes (consumidor) */
    int tail;              /* Índice para insertar mensajes (productor) */
    int count;             /* Número actual de mensajes en la cola */
    int next_msg_id;       /* ID para el siguiente mensaje */
    message_t messages[MAX_QUEUE_SIZE]; /* Array circular de mensajes */
} queue_t; /* Cola de mensajes */

/* Estructura para manejar recursos de la cola */ /* Cola de mensajes */
typedef struct {
    int shm_id;            /* ID del segmento de memoria compartida */
    int sem_id;            /* ID del conjunto de semáforos */
    queue_t *queue;        /* Puntero a la cola en memoria compartida */
    FILE *log_file;        /* Archivo de registro */
} message_queue_t; /* Cola de mensajes */

/* Estructura para mantener el estado de los clientes */
typedef struct {
    int socket;             /* Socket del cliente */
    int type;               /* Tipo: productor o consumidor */
    int consumer_id;        /* ID único para consumidores */
    int last_msg_offset;    /* Último mensaje recibido (offset) para consumidores */
    time_t last_activity;   /* Última actividad para detectar desconexiones */
    bool active;            /* Indica si el cliente está activo */
    char group[MAX_GROUP_NAME];
} client_t;

/* Estructura para el control del broker */
typedef struct {
    message_queue_t *mq;    /* Cola de mensajes */
    client_t clients[MAX_CLIENTS]; /* Arreglo de clientes conectados */
    int num_clients;        /* Número actual de clientes */
    int next_consumer_id;   /* ID para el siguiente consumidor */
    pthread_mutex_t clients_mutex; /* Mutex para proteger acceso a clientes */
    bool running;           /* Flag para controlar la ejecución del broker */
    char *persistence_file; /* Archivo para persistencia de mensajes */
} broker_t;

/* Estructura para la cola de tareas, para pool de hilos */
typedef struct task_node{
    int task_type;          /* tipo de tarea: cliente, distribuidor, etc */
    int client_socket;      /* socket del cliente para tareas del cliente */
    time_t scheduled_time;  /* tiempo programado para tareas periodicas */
    struct task_node *next;
} task_node;

/* Estructura para el pool de hilos */
typedef struct {
    pthread_t threads[THREAD_POOL_SIZE];  /* Hilos del pool */
    task_node *task_queue;                /* Cola de tareas (clientes a atender) */
    pthread_mutex_t queue_mutex;          /* Mutex para proteger la cola */
    pthread_cond_t queue_not_empty;       /* Condición para señalizar cuando hay tareas */
    pthread_cond_t queue_not_full;        /* Condición para señalizar cuando hay espacio */
    int queue_size;                       /* Número actual de tareas en la cola */
    int max_queue_size;                   /* Tamaño máximo de la cola */
    bool shutdown;                        /* Flag para indicar apagado del pool */
} thread_pool_t;

/* Variables globales */
broker_t broker; /* Broker */
int server_fd = -1; /* Descriptor del socket del servidor - VARIABLE GLOBAL */
thread_pool_t thread_pool; /* Pool de hilos */

/* Operación P (wait/decrement) sobre un semáforo */ /* Cola de mensajes */
static int sem_p(int sem_id, int sem_num) {
    struct sembuf op;
    op.sem_num = sem_num;
    op.sem_op = -1;  /* Decrementar */
    op.sem_flg = 0;
    return semop(sem_id, &op, 1);
} /* Cola de mensajes */

/* Operación V (signal/increment) sobre un semáforo */ /* Cola de mensajes */
static int sem_v(int sem_id, int sem_num) {
    struct sembuf op;
    op.sem_num = sem_num;
    op.sem_op = 1;   /* Incrementar */
    op.sem_flg = 0;
    return semop(sem_id, &op, 1);
} /* Cola de mensajes */

/* Escribe un mensaje en el archivo de registro */ /* Cola de mensajes */
static void log_message(FILE *log_file, const message_t *msg, const char *action) {
    char timestamp[64];
    struct tm *tm_info;
    
    tm_info = localtime(&msg->timestamp);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);
    
    fprintf(log_file, "[%s] %s - ID: %d, Size: %zu, Content: %.*s\n",
            timestamp, action, msg->msg_id, msg->size, (int)msg->size, msg->content);
    fflush(log_file);
} /* Cola de mensajes */

/**
 * Inicializa la cola de mensajes
 * @return Puntero a la estructura de cola o NULL en caso de error
 */
message_queue_t *mq_init() {
    message_queue_t *mq = NULL;
    
    /* Reservar memoria para la estructura de control */
    mq = (message_queue_t *)malloc(sizeof(message_queue_t));
    if (!mq) {
        perror("Error al reservar memoria para la cola");
        return NULL;
    }
    
    /* Crear o acceder al segmento de memoria compartida */
    /* Intentar limpiar primero en caso de quedarse huérfano */
    int existing_id = shmget(SHM_KEY, 0, 0666);
    if (existing_id != -1) {
        struct shmid_ds shm_info;
        if (shmctl(existing_id, IPC_STAT, &shm_info) == 0) {
            /* Verificar si está huérfano (nattch = 0) */
            if (shm_info.shm_nattch == 0) {
                shmctl(existing_id, IPC_RMID, NULL);
            }
        }
    }
    
    /* Ahora crear la nueva memoria compartida */
    mq->shm_id = shmget(SHM_KEY, sizeof(queue_t), IPC_CREAT | 0666);
    if (mq->shm_id == -1) {
        perror("Error al crear memoria compartida");
        free(mq);
        return NULL;
    }
    
    /* Adjuntar el segmento de memoria compartida */
    mq->queue = (queue_t *)shmat(mq->shm_id, NULL, 0);
    if (mq->queue == (queue_t *)-1) {
        perror("Error al adjuntar memoria compartida");
        shmctl(mq->shm_id, IPC_RMID, NULL);
        free(mq);
        return NULL;
    }
    
    /* Crear o acceder al conjunto de semáforos */
    
    /* Intentar crear exclusivamente primero */
    mq->sem_id = semget(SEM_KEY, SEM_COUNT, IPC_CREAT | IPC_EXCL | 0666);
    int is_first = 0;
    
    if (mq->sem_id != -1) {
        /* Es la primera vez - inicializar semáforos */
        is_first = 1;
    } else if (errno == EEXIST) {
        /* Ya existe - obtener el ID existente */
        mq->sem_id = semget(SEM_KEY, SEM_COUNT, 0666);
        if (mq->sem_id == -1) {
            perror("Error al obtener semáforos existentes");
            shmdt(mq->queue);
            shmctl(mq->shm_id, IPC_RMID, NULL);
            free(mq);
            return NULL;
        }
        is_first = 0;
    } else {
        /* Error real */
        perror("Error al crear semáforos");
        shmdt(mq->queue);
        shmctl(mq->shm_id, IPC_RMID, NULL);
        free(mq);
        return NULL;
    }
    
    /* Inicializar semáforos si es la primera vez */
    if (is_first) {
        union semun {
            int val;
            struct semid_ds *buf;
            unsigned short *array;
        } arg;
        
        /* Mutex: inicializado a 1 (disponible) */
        arg.val = 1;
        
        if (semctl(mq->sem_id, SEM_MUTEX, SETVAL, arg) == -1) {
            perror("Error al inicializar semáforo mutex");
            goto cleanup;
        }
        
        /* Empty: inicializado a MAX_QUEUE_SIZE (todos los slots están vacíos) */
        arg.val = MAX_QUEUE_SIZE;
        
        if (semctl(mq->sem_id, SEM_EMPTY, SETVAL, arg) == -1) {
            perror("Error al inicializar semáforo empty");
            goto cleanup;
        }
        
        /* Full: inicializado a 0 (no hay mensajes) */
        arg.val = 0;
        
        if (semctl(mq->sem_id, SEM_FULL, SETVAL, arg) == -1) {
            perror("Error al inicializar semáforo full");
            goto cleanup;
        }
        
        /* Inicializar estructura de la cola */
        mq->queue->head = 0;
        mq->queue->tail = 0;
        mq->queue->count = 0;
        mq->queue->next_msg_id = 1;
        
        /* Registrar inicialización en el log */
        fprintf(stderr, "Cola inicializada: creando nueva estructura\n");
        fflush(stderr);
    }
    
    /* Abrir archivo de registro */
    mq->log_file = fopen(LOG_FILE, "a+");
    if (!mq->log_file) {
        perror("Error al abrir archivo de registro");
        goto cleanup;
    }
    
    /* Registro de inicialización */
    fprintf(mq->log_file, "[%ld] Cola de mensajes inicializada\n", time(NULL));
    fflush(mq->log_file);
    
    return mq;

cleanup:
    semctl(mq->sem_id, 0, IPC_RMID, NULL);
    shmdt(mq->queue);
    shmctl(mq->shm_id, IPC_RMID, NULL);
    free(mq);
    return NULL;
}

/**
 * Libera los recursos de la cola de mensajes
 * @param mq Puntero a la estructura de cola
 */
void mq_cleanup(message_queue_t *mq) {
    if (!mq) return;
    
    /* Cerrar archivo de registro */
    if (mq->log_file) {
        fprintf(mq->log_file, "[%ld] Cola de mensajes finalizada\n", time(NULL));
        fflush(mq->log_file);
        fclose(mq->log_file);
    }
    
    /* Desconectar de la memoria compartida */
    shmdt(mq->queue);
    
    /* Eliminar la memoria compartida */
    if (shmctl(mq->shm_id, IPC_RMID, NULL) == -1) {
        perror("Error al eliminar memoria compartida");
    }
    
    /* Eliminar el conjunto de semáforos */
    if (semctl(mq->sem_id, 0, IPC_RMID) == -1) {
        perror("Error al eliminar semáforos");
    }    
    
    /* Liberar la memoria de la estructura */
    free(mq);
}

/**
 * Inserta un mensaje en la cola (productor)
 * @param mq Puntero a la estructura de cola
 * @param content Contenido del mensaje
 * @param size Tamaño del contenido
 * @return ID del mensaje o -1 en caso de error
 */
int mq_send(message_queue_t *mq, const char *content, size_t size) {
    if (!mq || !content || size == 0 || size > MAX_MSG_SIZE) {
        errno = EINVAL;
        return -1;
    }
    
    /* Esperar si la cola está llena */
    if (sem_p(mq->sem_id, SEM_EMPTY) == -1) {
        perror("Error al esperar espacio en la cola");
        return -1;
    }
    
    /* Obtener acceso exclusivo */
    if (sem_p(mq->sem_id, SEM_MUTEX) == -1) {
        perror("Error al obtener mutex");
        sem_v(mq->sem_id, SEM_EMPTY);  /* Devolver el permiso */
        return -1;
    }
    
    /* Preparar el mensaje - nota que estamos dentro de la sección crítica (mutex) */
    message_t *msg = &mq->queue->messages[mq->queue->tail];
    
    /* Asignar ID único al mensaje - esto es seguro porque estamos dentro de la sección crítica */
    int msg_id = mq->queue->next_msg_id++;
    msg->msg_id = msg_id;
    
    msg->timestamp = time(NULL);
    msg->size = size;
    memcpy(msg->content, content, size);
    
    /* Asegurarse de que el contenido termine con nulo si es un string */
    if (size < MAX_MSG_SIZE) {
        msg->content[size] = '\0';
    } else {
        msg->content[MAX_MSG_SIZE - 1] = '\0';
    }
    
    /* Actualizar índice de la cola (circular) */
    mq->queue->tail = (mq->queue->tail + 1) % MAX_QUEUE_SIZE;
    mq->queue->count++;
    
    /* Registrar el mensaje */
    log_message(mq->log_file, msg, "SEND");
    
    /* Liberar el mutex */
    if (sem_v(mq->sem_id, SEM_MUTEX) == -1) {
        perror("Error al liberar mutex");
        return -1;
    }
    
    /* Señalizar que hay un nuevo mensaje */
    if (sem_v(mq->sem_id, SEM_FULL) == -1) {
        perror("Error al señalizar nuevo mensaje");
        return -1;
    }
    
    return msg_id;
}

/**
 * Extrae un mensaje de la cola (consumidor)
 * @param mq Puntero a la estructura de cola
 * @param buffer Buffer para almacenar el mensaje
 * @param max_size Tamaño máximo del buffer
 * @return Tamaño del mensaje o -1 en caso de error
 */
ssize_t mq_receive(message_queue_t *mq, char *buffer, size_t max_size) {
    if (!mq || !buffer || max_size == 0) {
        errno = EINVAL;
        return -1;
    }
    
    /* Esperar si no hay mensajes */
    if (sem_p(mq->sem_id, SEM_FULL) == -1) {
        perror("Error al esperar mensajes");
        return -1;
    }
    
    /* Obtener acceso exclusivo */
    if (sem_p(mq->sem_id, SEM_MUTEX) == -1) {
        perror("Error al obtener mutex");
        sem_v(mq->sem_id, SEM_FULL);  /* Devolver el permiso */
        return -1;
    }
    
    /* Obtener el mensaje */
    message_t *msg = &mq->queue->messages[mq->queue->head];
    
    /* Copiar el contenido al buffer */
    size_t copy_size = (msg->size < max_size) ? msg->size : max_size;
    memcpy(buffer, msg->content, copy_size);
    
    /* Registrar la recepción */
    log_message(mq->log_file, msg, "RECEIVE");
    
    /* Actualizar índice de la cola (circular) */
    mq->queue->head = (mq->queue->head + 1) % MAX_QUEUE_SIZE;
    mq->queue->count--;
    
    /* Liberar el mutex */
    if (sem_v(mq->sem_id, SEM_MUTEX) == -1) {
        perror("Error al liberar mutex");
        return -1;
    }
    
    /* Señalizar que hay un espacio libre */
    if (sem_v(mq->sem_id, SEM_EMPTY) == -1) {
        perror("Error al señalizar espacio libre");
        return -1;
    }
    
    return copy_size;
}

/**
 * Obtiene el número de mensajes actuales en la cola
 * @param mq Puntero a la estructura de cola
 * @return Número de mensajes o -1 en caso de error
 */
int mq_get_count(message_queue_t *mq) {
    if (!mq) {
        errno = EINVAL;
        return -1;
    }
    
    int count;
    
    /* Obtener acceso exclusivo */
    if (sem_p(mq->sem_id, SEM_MUTEX) == -1) {
        perror("Error al obtener mutex");
        return -1;
    }
    
    /* Leer el contador */
    count = mq->queue->count;
    
    /* Liberar el mutex */
    if (sem_v(mq->sem_id, SEM_MUTEX) == -1) {
        perror("Error al liberar mutex");
        return -1;
    }
    
    return count;
}

/* Manejador de señales para finalización ordenada */
void signal_handler(int sig) {
    static volatile sig_atomic_t cleaning_up = 0;
    
    /* Evitar entrar múltiples veces en la limpieza */
    if (cleaning_up) {
        return;
    }
    
    cleaning_up = 1;
    
    printf("\nRecibida señal %d. Finalizando broker...\n", sig);
    fflush(stdout);
    
    broker.running = false;
    
    /* No cerrar el socket del servidor aquí, dejarlo para cleanup_broker */
    /* El resto de la limpieza se hará en cleanup_broker() */
}

/* Guarda los offsets de los consumidores */
void save_offsets() {
    char offset_file[256];
    sprintf(offset_file, "%s.offsets", broker.persistence_file);
    
    FILE *file = fopen(offset_file, "w");
    if (!file) {
        perror("Error al abrir archivo de offsets");
        return;
    }
    
    /* Guardar next_consumer_id primero */
    fprintf(file, "NEXT_ID:%d\n", broker.next_consumer_id);
    
    /* Guardar offsets de consumidores activos */
    pthread_mutex_lock(&broker.clients_mutex);
    
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (broker.clients[i].active && broker.clients[i].type == CLIENT_TYPE_CONSUMER) {
            fprintf(file, "CONSUMER:%d:%d\n", 
                   broker.clients[i].consumer_id, 
                   broker.clients[i].last_msg_offset);
        }
    }
    
    pthread_mutex_unlock(&broker.clients_mutex);
    
    fclose(file);
}

/* Función para agregar una tarea (socket de cliente) a la cola */
int thread_pool_add_task(int task_type, int client_socket) {
    task_node *new_task = NULL;
    
    /* Crear nueva tarea */
    new_task = (task_node *)malloc(sizeof(task_node));
    if (!new_task) {
        perror("Error al asignar memoria para nueva tarea");
        return -1;
    }
    
    new_task->task_type = task_type;
    new_task->client_socket = client_socket;
    new_task->scheduled_time = 0; /* No es una tarea programada */
    new_task->next = NULL;
    
    /* Añadir tarea a la cola */
    pthread_mutex_lock(&thread_pool.queue_mutex);
    
    /* Esperar si la cola está llena */
    while (thread_pool.queue_size >= thread_pool.max_queue_size && !thread_pool.shutdown) {
        pthread_cond_wait(&thread_pool.queue_not_full, &thread_pool.queue_mutex);
    }
    
    /* Si el pool está en proceso de cierre, no agregar tarea */
    if (thread_pool.shutdown) {
        pthread_mutex_unlock(&thread_pool.queue_mutex);
        free(new_task);
        return -1;
    }
    
    /* Insertar al final de la cola */
    if (thread_pool.task_queue == NULL) {
        thread_pool.task_queue = new_task;
    } else {
        task_node *curr = thread_pool.task_queue;
        while (curr->next != NULL) {
            curr = curr->next;
        }
        curr->next = new_task;
    }
    
    thread_pool.queue_size++;
    
    /* Señalizar que hay una tarea disponible */
    pthread_cond_signal(&thread_pool.queue_not_empty);
    
    pthread_mutex_unlock(&thread_pool.queue_mutex);
    
    return 0;
}

/* Función para extraer una tarea de la cola */
task_node thread_pool_get_task() {
    task_node result;
    
    /* Intentar obtener el mutex con timeout */
    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_sec += 2; // 2 segundos de timeout
    
    if (pthread_mutex_timedlock(&thread_pool.queue_mutex, &timeout) != 0) {
        /* No pudimos obtener el lock en tiempo razonable */
        result.task_type = -1;  /* Indicador de error */
        return result;
    }
    
    /* Esperar si la cola está vacía */
    while (thread_pool.queue_size == 0 && !thread_pool.shutdown) {
        /* Usar timed wait en lugar de wait indefinido */
        clock_gettime(CLOCK_REALTIME, &timeout);
        timeout.tv_sec += 1; // 1 segundo de timeout para el wait
        
        int wait_result = pthread_cond_timedwait(&thread_pool.queue_not_empty, 
                                              &thread_pool.queue_mutex, 
                                              &timeout);
        
        if (wait_result != 0) {
            /* Timeout en la espera, verificar si debemos terminar */
            if (thread_pool.shutdown) {
                break;
            }
        }
    }
    
    /* Si el pool está cerrando y no hay tareas, retornar error */
    if (thread_pool.queue_size == 0 && thread_pool.shutdown) {
        pthread_mutex_unlock(&thread_pool.queue_mutex);
        result.task_type = -1;  /* Indicador de error */
        return result;
    }
    
    /* Si no hay tareas (posiblemente por timeout) pero no estamos cerrando, 
       devolver un valor que indique verificar de nuevo */
    if (thread_pool.queue_size == 0) {
        pthread_mutex_unlock(&thread_pool.queue_mutex);
        result.task_type = -2;  /* Indicador de "verificar de nuevo" */
        return result;
    }
    
    time_t current_time = time(NULL);
    task_node *prev = NULL;
    task_node *curr = thread_pool.task_queue;
    task_node *selected = NULL;
    task_node *selected_prev = NULL;
    
    /* Buscar la primera tarea programada que esté lista para ejecutarse */
    while (curr != NULL) {
        if (curr->scheduled_time > 0 && curr->scheduled_time <= current_time) {
            selected = curr;
            selected_prev = prev;
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    
    /* Si no hay tareas programadas listas, usar la primera tarea de la cola */
    if (selected == NULL) {
        selected = thread_pool.task_queue;
        selected_prev = NULL;
    }
    
    /* Copiar datos de la tarea seleccionada */
    result.task_type = selected->task_type;
    result.client_socket = selected->client_socket;
    result.scheduled_time = selected->scheduled_time;
    
    /* Eliminar tarea de la cola */
    if (selected_prev == NULL) {
        thread_pool.task_queue = selected->next;
    } else {
        selected_prev->next = selected->next;
    }
    
    thread_pool.queue_size--;
    
    /* Señalizar que hay espacio en la cola */
    pthread_cond_signal(&thread_pool.queue_not_full);
    
    pthread_mutex_unlock(&thread_pool.queue_mutex);
    
    /* Liberar nodo de la tarea */
    free(selected);
    
    return result;
}

/* Programa una tarea periódica */
void schedule_periodic_task(int task_type, int seconds_delay) {
    // Verificar si el pool está en proceso de cierre
    pthread_mutex_lock(&thread_pool.queue_mutex);
    if (thread_pool.shutdown) {
        pthread_mutex_unlock(&thread_pool.queue_mutex);
        return; // No programar más tareas si estamos cerrando
    }
    pthread_mutex_unlock(&thread_pool.queue_mutex);
    
    task_node *new_task = (task_node *)malloc(sizeof(task_node));
    if (!new_task) {
        perror("Error al asignar memoria para tarea periódica");
        return;
    }
    
    new_task->task_type = task_type;
    new_task->client_socket = -1;
    new_task->scheduled_time = time(NULL) + seconds_delay;
    new_task->next = NULL;
    
    // Intentar adquirir el mutex con timeout
    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_sec += 2; // 2 segundos de timeout
    
    int lock_result = pthread_mutex_timedlock(&thread_pool.queue_mutex, &timeout);
    
    if (lock_result != 0) {
        free(new_task);
        return;
    }
    
    // Verificar de nuevo, podría haber cambiado mientras esperábamos
    if (thread_pool.shutdown) {
        pthread_mutex_unlock(&thread_pool.queue_mutex);
        free(new_task);
        return;
    }
    
    /* Añadir al final de la cola */
    if (thread_pool.task_queue == NULL) {
        thread_pool.task_queue = new_task;
    } else {
        task_node *curr = thread_pool.task_queue;
        while (curr->next != NULL) {
            curr = curr->next;
        }
        curr->next = new_task;
    }
    
    thread_pool.queue_size++;
    
    pthread_cond_signal(&thread_pool.queue_not_empty);
    
    pthread_mutex_unlock(&thread_pool.queue_mutex);
}

/* Función para cerrar el pool de hilos */
void thread_pool_shutdown() {
    printf("[Pool Shutdown] Iniciando cierre del pool de hilos\n");
    
    pthread_mutex_lock(&thread_pool.queue_mutex);
    thread_pool.shutdown = true;
    printf("[Pool Shutdown] Indicador de shutdown activado\n");
    
    /* Despertar todos los hilos que puedan estar esperando */
    pthread_cond_broadcast(&thread_pool.queue_not_empty);
    pthread_cond_broadcast(&thread_pool.queue_not_full);
    pthread_mutex_unlock(&thread_pool.queue_mutex);
    
    /* Esperar a que finalicen todos los hilos */
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        void *thread_result;
        int join_result = pthread_join(thread_pool.threads[i], &thread_result);
        
        if (join_result != 0) {
            printf("[Pool Shutdown] Error al unir hilo %d (error: %d)\n", i, join_result);
        }
    }
    
    /* Limpiar cola de tareas */
    pthread_mutex_lock(&thread_pool.queue_mutex);
    task_node *current = thread_pool.task_queue;
    while (current != NULL) {
        task_node *next = current->next;
        
        /* Solo cerrar socket para tareas de cliente */
        if (current->task_type == TASK_CLIENT_HANDLER && current->client_socket >= 0) {
            close(current->client_socket);
        }
        
        free(current);
        current = next;
    }
    thread_pool.task_queue = NULL;
    pthread_mutex_unlock(&thread_pool.queue_mutex);
    
    /* Destruir mutex y variables de condición */
    pthread_mutex_destroy(&thread_pool.queue_mutex);
    pthread_cond_destroy(&thread_pool.queue_not_empty);
    pthread_cond_destroy(&thread_pool.queue_not_full);
    
    printf("Pool de hilos finalizado\n");
}

/* Libera recursos utilizados por el broker */
void cleanup_broker() {
    /* Establecer flag de parada para que otros hilos terminen sus bucles */
    broker.running = false;
    
    /* Cerrar el socket del servidor si está abierto */
    if (server_fd > 0) {
        close(server_fd);
        server_fd = -1;
    }
    
    /* Cerrar sockets de clientes */
    if (pthread_mutex_lock(&broker.clients_mutex) == 0) {  // Proteger con try-lock
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (broker.clients[i].active && broker.clients[i].socket != -1) {
                close(broker.clients[i].socket);
                broker.clients[i].active = false;
                broker.clients[i].socket = -1;
            }
        }
        pthread_mutex_unlock(&broker.clients_mutex);
    }
    
    /* Guardar offsets de consumidores */
    if (broker.persistence_file != NULL) {
        save_offsets();
    }
    
    /* Cerrar el pool de hilos antes de liberar la cola de mensajes */
    thread_pool_shutdown();
    
    /* Liberar cola de mensajes */
    if (broker.mq) {
        mq_cleanup(broker.mq);
        broker.mq = NULL;
    }
    
    /* Liberar mutex */
    pthread_mutex_destroy(&broker.clients_mutex);
    
    /* Liberar string del archivo de persistencia */
    if (broker.persistence_file) {
        free(broker.persistence_file);
        broker.persistence_file = NULL;
    }
}

/* Configurar manejadores de señales */
void handle_signals() {
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    
    sigaction(SIGINT, &sa, NULL);   /* Ctrl+C */
    sigaction(SIGTERM, &sa, NULL);  /* kill */
}

/* Guarda un mensaje en el archivo de persistencia */
void save_message_to_persistence(int msg_id, const char *content, size_t size) {
    FILE *file = fopen(broker.persistence_file, "a");
    if (!file) {
        perror("Error al abrir archivo de persistencia");
        return;
    }
    
    /* Formato: MSG_ID:TIMESTAMP:SIZE:CONTENT */
    fprintf(file, "%d:%ld:%zu:", msg_id, time(NULL), size);
    fwrite(content, 1, size, file);
    fprintf(file, "\n");
    
    fclose(file);
}

/* Función de atención a clientes */
void *client_handler(void *client_socket_ptr) {
    int client_socket = *((int *)client_socket_ptr);
    
    char buffer[BUFFER_SIZE];
    char response[BUFFER_SIZE];
    ssize_t bytes_read;
    int client_index = -1;
    
    /* Establecer timeout para operaciones de socket */
    struct timeval tv;
    tv.tv_sec = 5;  /* 5 segundos de timeout */
    tv.tv_usec = 0;
    
    if (setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        /* Si falla, continuamos pero notamos posibles problemas de timeout */
        close(client_socket);
        return NULL;
    }
    
    /* Esperar el mensaje de registro inicial */
    memset(buffer, 0, BUFFER_SIZE);
    bytes_read = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);
    
    if (bytes_read <= 0) {
        close(client_socket);
        return NULL;
    }
    
    /* Asegurar que buffer termina en nulo */   
    buffer[bytes_read] = '\0';
    
    /* Procesar mensaje de registro */
int client_type = 0;
int consumer_id = 0;
char grupo[MAX_GROUP_NAME] = {0};  // <- Agregamos una variable temporal

if (strncmp(buffer, "REGISTER:PRODUCER", 17) == 0) {
    client_type = CLIENT_TYPE_PRODUCER;
} else if (strncmp(buffer, "REGISTER:CONSUMER:", 18) == 0) {
    client_type = CLIENT_TYPE_CONSUMER;

    char *grupo_recibido = &buffer[18];
    if (strlen(grupo_recibido) >= MAX_GROUP_NAME) {
        grupo_recibido[MAX_GROUP_NAME - 1] = '\0';
    }
    strncpy(grupo, grupo_recibido, MAX_GROUP_NAME);  // Guardamos temporalmente
} else {
    /* Mensaje de registro inválido */
    sprintf(response, "ERROR:Invalid registration message");
    send(client_socket, response, strlen(response), 0);
    close(client_socket);
    return NULL;
}
    
    /* Buscar un slot disponible para el cliente con manejo de mutex adecuado */
    if (pthread_mutex_lock(&broker.clients_mutex) != 0) {
        /* Error al obtener el mutex, no podemos continuar de forma segura */
        sprintf(response, "ERROR:Internal server error");
        send(client_socket, response, strlen(response), 0);
        close(client_socket);
        return NULL;
    }
    
    bool slot_found = false;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!broker.clients[i].active) {
            client_index = i;
            if (client_type == CLIENT_TYPE_CONSUMER) {
    strncpy(broker.clients[i].group, grupo, MAX_GROUP_NAME);
}  
            broker.clients[i].socket = client_socket;
            broker.clients[i].type = client_type;
            broker.clients[i].active = true;
            broker.clients[i].last_activity = time(NULL);
            
            if (client_type == CLIENT_TYPE_CONSUMER) {
                if (consumer_id > 0) {
                    /* Usar ID proporcionado si es válido */
                    broker.clients[i].consumer_id = consumer_id;
                    /* Actualizar next_consumer_id si necesario */
                    if (consumer_id >= broker.next_consumer_id) {
                        broker.next_consumer_id = consumer_id + 1;
                    }
                } else {
                    /* Asignar nuevo ID */
                    broker.clients[i].consumer_id = broker.next_consumer_id++;
                }
                broker.clients[i].last_msg_offset = 0; /* Empezar desde el principio */
            }
            
            broker.num_clients++;
            slot_found = true;
            break;
        }
    }
    
    pthread_mutex_unlock(&broker.clients_mutex);
    
    if (!slot_found) {
        /* No hay slots disponibles */
        sprintf(response, "ERROR:Server is full");
        send(client_socket, response, strlen(response), 0);
        close(client_socket);
        return NULL;
    }
    
    /* Enviar confirmación con ID asignado */
    memset(response, 0, BUFFER_SIZE);
    if (client_type == CLIENT_TYPE_PRODUCER) {
        sprintf(response, "OK:PRODUCER");
    } else {
        sprintf(response, "OK:CONSUMER:%d", broker.clients[client_index].consumer_id);
    }

    /* Asegurar que la respuesta se envía completamente */
    ssize_t bytes_sent = send(client_socket, response, strlen(response), 0);
    if (bytes_sent <= 0) {
        pthread_mutex_lock(&broker.clients_mutex);
        if (client_index >= 0 && client_index < MAX_CLIENTS && broker.clients[client_index].active) {
            broker.clients[client_index].active = false;
            broker.clients[client_index].socket = -1;
            broker.num_clients--;
        }
        pthread_mutex_unlock(&broker.clients_mutex);
        close(client_socket);
        return NULL;
    }

    /* Bucle principal de atención al cliente */
    fd_set read_fds;
    struct timeval select_timeout;
    int select_result;
    
    while (broker.running) {
        /* Preparar el conjunto de descriptores y timeout para select */
        FD_ZERO(&read_fds);
        FD_SET(client_socket, &read_fds);
        
        select_timeout.tv_sec = 1;  /* Timeout de 1 segundo */
        select_timeout.tv_usec = 0;
        
        select_result = select(client_socket + 1, &read_fds, NULL, NULL, &select_timeout);
        
        if (select_result < 0) {
            if (errno == EINTR) {
                continue;  /* Interrumpido por señal */
            }
            break;  /* Error grave */
        }
        
        if (select_result == 0) {
            /* Timeout, verificar si el broker sigue activo */
            if (!broker.running) {
                break;
            }
            continue;
        }
        
        /* Hay datos disponibles para leer */
        memset(buffer, 0, BUFFER_SIZE);
        bytes_read = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);     
        
        if (bytes_read <= 0) {
            if (bytes_read == 0) {
                /* Cliente desconectado normalmente */
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    /* No hay datos disponibles, continuar */
                    continue;
                }
                /* Error en recv */
            }
            break;
        }   
        
        /* Asegurar que buffer termina en nulo */
        buffer[bytes_read] = '\0';
        
        /* Actualizar tiempo de última actividad de forma segura */
        if (pthread_mutex_lock(&broker.clients_mutex) == 0) {
            if (client_index >= 0 && client_index < MAX_CLIENTS && broker.clients[client_index].active) {
                broker.clients[client_index].last_activity = time(NULL);
            }
            pthread_mutex_unlock(&broker.clients_mutex);
        }
        
        /* Procesar mensaje según tipo de cliente */
        if (client_type == CLIENT_TYPE_PRODUCER) {
            /* Productor: enviar mensaje a la cola */
            if (strncmp(buffer, "SEND:", 5) == 0) {
                int msg_id = mq_send(broker.mq, buffer + 5, bytes_read - 5);
                
                memset(response, 0, BUFFER_SIZE);
                if (msg_id >= 0) {
                    sprintf(response, "OK:MSG_ID:%d", msg_id);
                    
                    /* Guardar mensaje en persistencia */
                    save_message_to_persistence(msg_id, buffer + 5, bytes_read - 5);
                } else {
                    sprintf(response, "ERROR:Failed to send message");
                }
                
                /* Asegurar que la respuesta se envía completamente */
                bytes_sent = send(client_socket, response, strlen(response), 0);
                if (bytes_sent <= 0) {
                    /* Error al enviar respuesta, probablemente cliente desconectado */
                    break;
                }
            } else {
                /* Comando desconocido */
                memset(response, 0, BUFFER_SIZE);
                sprintf(response, "ERROR:Unknown command");
                
                bytes_sent = send(client_socket, response, strlen(response), 0);
                if (bytes_sent <= 0) {
                    /* Error al enviar respuesta */
                    break;
                }
            }
        }

//codigo para el consumidor con get



else if (client_type == CLIENT_TYPE_CONSUMER) {
    if (strncmp(buffer, "GET", 3) == 0) {
        char msg_buffer[MAX_MSG_SIZE] = {0};

        ssize_t msg_size = mq_receive(broker.mq, msg_buffer, sizeof(msg_buffer));

        if (msg_size > 0) {
            snprintf(response, sizeof(response), "Mensaje recibido: %s", msg_buffer);
        } else {
            snprintf(response, sizeof(response), "ERROR:No hay mensajes");
        }

        send(client_socket, response, strlen(response), 0);
    } else {
        snprintf(response, sizeof(response), "ERROR:Unknown command");
        send(client_socket, response, strlen(response), 0);
    }
}
        /* Pequeña pausa para evitar uso excesivo de CPU */
        usleep(1000);  /* 1ms */
    }
    
    /* Eliminar cliente de la lista de forma segura */
    if (pthread_mutex_lock(&broker.clients_mutex) == 0) {
        if (client_index >= 0 && client_index < MAX_CLIENTS && broker.clients[client_index].active) {
            broker.clients[client_index].active = false;
            broker.clients[client_index].socket = -1;
            broker.num_clients--;
        }
        pthread_mutex_unlock(&broker.clients_mutex);
    }
    
    /* Cerrar socket */
    close(client_socket);
    
    return NULL;
}

/* Realiza la distribución de notificaciones de mensajes a consumidores */
void perform_message_distribution() {
    char notification[64];
    int count = mq_get_count(broker.mq);

    if (count <= 0) return;

    pthread_mutex_lock(&broker.clients_mutex);

    char notified_groups[MAX_CLIENTS][MAX_GROUP_NAME];
    int group_count = 0;

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!broker.clients[i].active || broker.clients[i].type != CLIENT_TYPE_CONSUMER)
            continue;

        // Ver si el grupo ya fue notificado
        bool already_notified = false;
        for (int j = 0; j < group_count; j++) {
            if (strncmp(broker.clients[i].group, notified_groups[j], MAX_GROUP_NAME) == 0) {
                already_notified = true;
                break;
            }
        }

        if (!already_notified) {
            // Notificar a este consumidor
            snprintf(notification, sizeof(notification), "NOTIFY:MESSAGES_AVAILABLE:%d", count);
            send(broker.clients[i].socket, notification, strlen(notification), 0);

            // Registrar grupo ya notificado
            strncpy(notified_groups[group_count], broker.clients[i].group, MAX_GROUP_NAME);
            group_count++;
        }
    }

    pthread_mutex_unlock(&broker.clients_mutex);
}

/* Verifica y desconecta clientes inactivos */
void check_inactive_clients() {
    const int TIMEOUT_SECONDS = 300;  /* 5 minutos de inactividad */
    time_t current_time = time(NULL);
    
    pthread_mutex_lock(&broker.clients_mutex);
    
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (broker.clients[i].active) {
            if (current_time - broker.clients[i].last_activity > TIMEOUT_SECONDS) {
                printf("Cliente en slot %d inactivo por más de %d segundos, desconectando\n", 
                      i, TIMEOUT_SECONDS);
                
                close(broker.clients[i].socket);
                broker.clients[i].active = false;
                broker.clients[i].socket = -1;
                broker.num_clients--;
            }
        }
    }
    
    pthread_mutex_unlock(&broker.clients_mutex);
}

/* Función que ejecutan los hilos trabajadores */
void *worker_thread(void *arg) {
    while (true) {
        /* Verificar primero si debemos terminar */
        if (!broker.running) {
            break;
        }
        
        /* Obtener una tarea para procesar */
        task_node task;
        
        /* Inicializar campos para seguridad */
        task.task_type = -2;
        task.client_socket = -1;
        task.scheduled_time = 0;
        task.next = NULL;
        
        task = thread_pool_get_task();
        
        /* Si el task_type es -1, el pool está cerrando */
        if (task.task_type == -1) {
            break;
        }
        
        /* Si el task_type es -2, timeout en espera de tarea, verificar si debemos terminar */
        if (task.task_type == -2) {
            pthread_mutex_lock(&thread_pool.queue_mutex);
            bool should_exit = thread_pool.shutdown || !broker.running;
            pthread_mutex_unlock(&thread_pool.queue_mutex);
            
            if (should_exit) {
                break;
            }
            
            /* Pequeña pausa para evitar CPU spinning */
            usleep(50000); /* 50ms de espera */
            continue;
        }
        
        /* Procesar según el tipo de tarea */
        switch (task.task_type) {
            case TASK_CLIENT_HANDLER:
                if (task.client_socket >= 0) {
                    client_handler(&task.client_socket);
                }
                break;
                
            case TASK_DISTRIBUTOR:
                perform_message_distribution();
                
                /* Verificar si el pool está cerrando antes de reprogramar */
                pthread_mutex_lock(&thread_pool.queue_mutex);
                bool distributor_should_continue = !thread_pool.shutdown && broker.running;
                pthread_mutex_unlock(&thread_pool.queue_mutex);
                
                if (distributor_should_continue) {
                    /* Volver a programar esta tarea periódica */
                    schedule_periodic_task(TASK_DISTRIBUTOR, 1); /* 1 segundo */
                }
                break;
                
            case TASK_PERSISTER:
                save_offsets();   
                
                /* Verificar si el pool está cerrando antes de reprogramar */
                pthread_mutex_lock(&thread_pool.queue_mutex);
                bool persister_should_continue = !thread_pool.shutdown && broker.running;
                pthread_mutex_unlock(&thread_pool.queue_mutex);
                
                if (persister_should_continue) {
                    /* Volver a programar esta tarea periódica */
                    schedule_periodic_task(TASK_PERSISTER, 5); /* 5 segundos */
                }
                break;
                
            case TASK_INACTIVE_CHECK:
                check_inactive_clients();
                
                /* Verificar si el pool está cerrando antes de reprogramar */
                pthread_mutex_lock(&thread_pool.queue_mutex);
                bool inactive_check_should_continue = !thread_pool.shutdown && broker.running;
                pthread_mutex_unlock(&thread_pool.queue_mutex);
                
                if (inactive_check_should_continue) {
                    /* Volver a programar esta tarea periódica */
                    schedule_periodic_task(TASK_INACTIVE_CHECK, 10); /* 10 segundos */
                }
                break;
                
            default:
                /* No imprimir mensaje, pero manejar caso desconocido */
                usleep(10000); /* 10ms de espera */
                break;
        }
        
        /* Verificar periódicamente si debemos terminar */
        if (!broker.running) {
            break;
        }
        
        pthread_mutex_lock(&thread_pool.queue_mutex);
        if (thread_pool.shutdown) {
            pthread_mutex_unlock(&thread_pool.queue_mutex);
            break;
        }
        pthread_mutex_unlock(&thread_pool.queue_mutex);
        
        /* Pequeña pausa entre iteraciones para evitar CPU spinning */
        usleep(1000); /* 1ms de pausa */
    }
    
    return NULL;
}

/* Función para inicializar el pool de hilos */
void thread_pool_init(int max_queue_size) {
  
    /* Inicializar estructura del pool */
    
    thread_pool.task_queue = NULL;
    thread_pool.queue_size = 0;
    thread_pool.max_queue_size = max_queue_size;
    thread_pool.shutdown = false;
    
    pthread_mutex_init(&thread_pool.queue_mutex, NULL);
    
    pthread_cond_init(&thread_pool.queue_not_empty, NULL);
    pthread_cond_init(&thread_pool.queue_not_full, NULL);
    
    /* Crear hilos trabajadores */
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        
        int result = pthread_create(&thread_pool.threads[i], NULL, worker_thread, (void*)(long)i);
        
        if (result != 0) {
            perror("Error al crear hilo del pool");
            exit(EXIT_FAILURE);
        }
        
    }
    
    printf("Pool de hilos inicializado con %d hilos\n", THREAD_POOL_SIZE);
}

/* Inicializa el broker y sus recursos */
void initialize_broker(const char *persistence_file) {

    /* Inicializar estructura del broker */

    memset(&broker, 0, sizeof(broker));
    broker.running = true;
    broker.next_consumer_id = 1;
    broker.persistence_file = strdup(persistence_file);
    
    /* Inicializar mutex */
    pthread_mutex_init(&broker.clients_mutex, NULL);
    
    /* Inicializar el pool de hilos */
    thread_pool_init(MAX_CLIENTS * 2);  /* Tamaño de cola = doble del máximo de clientes */
    
    /* Inicializar cola de mensajes */
    broker.mq = mq_init();
    if (!broker.mq) {
        fprintf(stderr, "Error al inicializar cola de mensajes\n");
        exit(EXIT_FAILURE);
    }

    /* Inicializar arreglo de clientes */
    for (int i = 0; i < MAX_CLIENTS; i++) {
        broker.clients[i].socket = -1;
        broker.clients[i].active = false;
    }
    
    printf("Broker inicializado correctamente\n");
}

/* Carga los offsets de los consumidores */
void load_offsets() {
    char offset_file[256];
    sprintf(offset_file, "%s.offsets", broker.persistence_file);
    
    FILE *file = fopen(offset_file, "r");
    if (!file) {
        /* Es normal que no exista al principio */
        return;
    }
    
    char line[256];
    
    /* Leer líneas del archivo */
    while (fgets(line, sizeof(line), file)) {
        if (strncmp(line, "NEXT_ID:", 8) == 0) {
            /* Restaurar next_consumer_id */
            broker.next_consumer_id = atoi(line + 8);
        } else if (strncmp(line, "CONSUMER:", 9) == 0) {
            /* Formato: CONSUMER:ID:OFFSET */
            char *token = strtok(line + 9, ":");
            if (token) {
                int consumer_id = atoi(token);
                token = strtok(NULL, ":");
                if (token) {
                    int offset = atoi(token);
                    
                    /* Buscar si este consumidor ya está conectado */
                    pthread_mutex_lock(&broker.clients_mutex);
                    for (int i = 0; i < MAX_CLIENTS; i++) {
                        if (broker.clients[i].active && 
                            broker.clients[i].type == CLIENT_TYPE_CONSUMER &&
                            broker.clients[i].consumer_id == consumer_id) {
                            
                            broker.clients[i].last_msg_offset = offset;
                            break;
                        }
                    }
                    pthread_mutex_unlock(&broker.clients_mutex);
                }
            }
        }
    }
    
    fclose(file);
}

/* Inicializa las tareas periódicas */
void initialize_periodic_tasks() {
    
    /* Programar primera ejecución de cada tarea periódica */
    schedule_periodic_task(TASK_DISTRIBUTOR, 1);    /* Cada 1 segundo */
    
    schedule_periodic_task(TASK_PERSISTER, 5);     /* Cada 5 segundos */
    
    schedule_periodic_task(TASK_INACTIVE_CHECK, 10); /* Cada 10 segundos */
    
}

/* Función para limpiar recursos IPC huérfanos */
void cleanup_orphaned_ipc() {
    int shm_id, sem_id;
    
    /* Intentar obtener la memoria compartida existente */
    shm_id = shmget(SHM_KEY, sizeof(queue_t), 0666);
    if (shm_id != -1) {
        printf("Encontrada memoria compartida huérfana, eliminando...\n");
        shmctl(shm_id, IPC_RMID, NULL);
    }
    
    /* Intentar obtener los semáforos existentes */
    sem_id = semget(SEM_KEY, SEM_COUNT, 0666);
    if (sem_id != -1) {
        printf("Encontrados semáforos huérfanos, eliminando...\n");
        semctl(sem_id, 0, IPC_RMID);
    }
}

int main(int argc, char *argv[]) {
    int new_socket;
    struct sockaddr_in address;
    int opt = 1;
    
    /* Limpiar recursos IPC huérfanos si existen */
    cleanup_orphaned_ipc();    

    /* Verificar argumentos */
    if (argc < 2) {
        fprintf(stderr, "Uso: %s <archivo_persistencia>\n", argv[0]);
        return EXIT_FAILURE;
    }
    
    /* Inicializar broker */
    initialize_broker(argv[1]);
    
    /* Configurar manejadores de señales */
    handle_signals();
    
    /* Crear socket */
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("Error al crear socket");
        cleanup_broker();
        return EXIT_FAILURE;
    }
    
    /* Configurar socket para reutilizar dirección y puerto */
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("Error en setsockopt");
        close(server_fd);
        cleanup_broker();
        return EXIT_FAILURE;
    }
    
    /* Configurar dirección */
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
    
    /* Vincular socket a puerto */
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Error en bind");
        close(server_fd);
        cleanup_broker();
        return EXIT_FAILURE;
    }
    
    /* Escuchar conexiones */
    if (listen(server_fd, 10) < 0) {
        perror("Error en listen");
        close(server_fd);
        cleanup_broker();
        return EXIT_FAILURE;
    }
    
    /* Configurar socket servidor como no bloqueante */
    int flags = fcntl(server_fd, F_GETFL, 0);
    if (flags == -1) {
        perror("Error en fcntl F_GETFL");
        close(server_fd);
        cleanup_broker();
        return EXIT_FAILURE;
    }
    
    int new_flags = flags | O_NONBLOCK;
    if (fcntl(server_fd, F_SETFL, new_flags) == -1) {
        perror("Error en fcntl F_SETFL");
        close(server_fd);
        cleanup_broker();
        return EXIT_FAILURE;
    }
    
    /* Verificar que se establecieron correctamente los flags */
    int check_flags = fcntl(server_fd, F_GETFL, 0);
    if ((check_flags & O_NONBLOCK) == 0) {
        fprintf(stderr, "Error: No se pudo configurar el socket como no bloqueante\n");
        close(server_fd);
        cleanup_broker();
        return EXIT_FAILURE;
    }
    
    printf("Broker iniciado en puerto %d\n", PORT);
    fflush(stdout);
    
    /* Cargar offsets de consumidores si existen */
    load_offsets();
    
    /* Iniciar tareas periódicas */
    initialize_periodic_tasks();
    
    /* Variables para el bucle principal */
    struct pollfd fds[1];
    int loop_count = 0;
    int max_error_count = 5;
    int consecutive_errors = 0;
    time_t last_error_time = 0;
    time_t current_time;
    
    /* Bucle principal para aceptar conexiones */
    while (broker.running) {
        loop_count++;
        
        /* Reiniciar contador de errores si ha pasado suficiente tiempo */
        current_time = time(NULL);
        if (last_error_time > 0 && current_time - last_error_time > 10) {
            consecutive_errors = 0;
        }
        
        /* Configurar poll() */
        fds[0].fd = server_fd;
        fds[0].events = POLLIN;
        fds[0].revents = 0;
        
        /* Llamar a poll() con timeout sensible */
        int activity = poll(fds, 1, 500);  /* 500ms para mejorar la reactividad */
        
        if (activity < 0) {
            if (errno == EINTR) {
                continue;  /* Interrumpido por señal, reintentar */
            } else {
                /* Error grave, incrementar contador y verificar umbral */
                consecutive_errors++;
                last_error_time = current_time;
                
                perror("Error en poll");
                
                if (consecutive_errors >= max_error_count) {
                    fprintf(stderr, "Demasiados errores consecutivos, saliendo\n");
                    break;
                }
                
                /* Breve pausa antes de reintentar */
                usleep(100000);  /* 100ms */
                continue;
            }
        }
        
        if (activity == 0) {
            /* Timeout, no hay actividad */
            continue;
        }
        
        /* Si hay actividad en el socket servidor */
        if (fds[0].revents & POLLIN) {
            /* Aceptar nueva conexión */
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            
            new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
            
            if (new_socket < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    /* No hay conexiones pendientes, es normal con socket no bloqueante */
                } else if (errno == EINTR) {
                    /* Interrumpido por señal */
                } else {
                    /* Error real, incrementar contador */
                    consecutive_errors++;
                    last_error_time = current_time;
                    perror("Error en accept");
                    
                    if (consecutive_errors >= max_error_count) {
                        fprintf(stderr, "Demasiados errores consecutivos en accept, saliendo\n");
                        break;
                    }
                }
                continue;
            }
            
            /* Resetear contador de errores cuando hay éxito */
            consecutive_errors = 0;
            
            /* Conexión aceptada, imprimir información */
            printf("Nueva conexión aceptada: %s:%d\n", 
                   inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
            
            /* Configurar socket cliente como no bloqueante */
            int client_flags = fcntl(new_socket, F_GETFL, 0);
            if (client_flags == -1) {
                perror("Error en fcntl F_GETFL para cliente");
                close(new_socket);
                continue;
            }
            
            if (fcntl(new_socket, F_SETFL, client_flags | O_NONBLOCK) == -1) {
                perror("Error en fcntl F_SETFL para cliente");
                close(new_socket);
                continue;
            }
            
            /* Agregar tarea al pool de hilos */
            int result = thread_pool_add_task(TASK_CLIENT_HANDLER, new_socket);
            
            if (result != 0) {
                perror("Error al agregar tarea al pool de hilos");
                close(new_socket);
            }
        } else if (fds[0].revents != 0) {
            /* Eventos inesperados, podría indicar problema con el socket */
            if (fds[0].revents & POLLERR) {
                fprintf(stderr, "Error en el socket del servidor (POLLERR)\n");
            }
            if (fds[0].revents & POLLHUP) {
                fprintf(stderr, "Conexión cerrada inesperadamente (POLLHUP)\n");
            }
            if (fds[0].revents & POLLNVAL) {
                fprintf(stderr, "Descriptor de archivo inválido (POLLNVAL)\n");
                /* El descriptor es inválido, no podemos continuar */
                broker.running = false;
                break;
            }
            
            /* Incrementar contador de errores */
            consecutive_errors++;
            last_error_time = current_time;
            
            if (consecutive_errors >= max_error_count) {
                fprintf(stderr, "Demasiados eventos inesperados consecutivos, saliendo\n");
                break;
            }
        }
        
        /* Pequeña pausa para evitar CPU spinning en bucles rápidos */
        usleep(1000);  /* 1ms */
    }
    
    printf("Saliendo del bucle principal\n");
    
    /* Limpiar recursos */
    cleanup_broker();
    
    printf("Broker finalizado correctamente\n");
    fflush(stdout);
    
    return EXIT_SUCCESS;
}
