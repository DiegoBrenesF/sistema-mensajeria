#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8080
#define BUFFER_SIZE 1024

int main() {
    int sockfd;
    struct sockaddr_in server_addr;
    char buffer[BUFFER_SIZE];

    // 1. Crear socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        perror("Error al crear socket");
        exit(EXIT_FAILURE);
    }

    // 2. Configurar dirección del broker
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        perror("Dirección inválida");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // 3. Conectarse al broker
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        perror("Error al conectar con el broker");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // 4. Enviar mensaje de registro como consumidor (¡termina con \n!)
    char *registro = "REGISTER:CONSUMER\n";
    if (send(sockfd, registro, strlen(registro), 0) == -1) {
        perror("Error al enviar mensaje de registro");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    printf("Mensaje de registro enviado\n");

    // 5. Esperar confirmación del broker
    memset(buffer, 0, BUFFER_SIZE);
    int bytes_recibidos = recv(sockfd, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_recibidos <= 0) {
        printf("No se recibió respuesta del broker\n");
        close(sockfd);
        return 1;
    }

    printf("Respuesta del broker: %s", buffer);

    // 6. Verificar respuesta del broker
    if (strncmp(buffer, "OK:CONSUMER", 11) != 0) {
        printf("ERROR: Registro inválido o rechazado\n");
        close(sockfd);
        return 1;
    }

    printf("Esperando notificaciones...\n");

    // 7. Esperar mensajes indefinidamente
    while (1) {
        memset(buffer, 0, BUFFER_SIZE);
        bytes_recibidos = recv(sockfd, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_recibidos <= 0) {
            printf("Desconectado del broker.\n");
            break;
        }

        printf("Mensaje recibido: %s\n", buffer);

        if (strncmp(buffer, "NOTIFY:MESSAGES_AVAILABLE", 25) == 0) {
            // Enviar solicitud de mensajes
            const char *get_cmd = "GET\n";
            if (send(sockfd, get_cmd, strlen(get_cmd), 0) == -1) {
                perror("Error al solicitar mensajes");
                break;
            }

            // Recibir contenido
            memset(buffer, 0, BUFFER_SIZE);
            int r = recv(sockfd, buffer, BUFFER_SIZE - 1, 0);
            if (r > 0) {
                printf("Contenido recibido: %s\n", buffer);
            } else {
                printf("No se recibió contenido del broker.\n");
                break;
            }
        }
    }

    close(sockfd);
    return 0;
}
