/*
 *  Author:     Group 6
 *  Date:       March 13, 2017
 *  Purpose:    A server to provide information of movies
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <json.h>

#include <my_global.h>
#include <mysql.h>

#include "../inc/movie.h"

void doprocessing (int sock);
void childDeath();
int messageHandler(char *buffer, int len, MYSQL *con);
MYSQL *setup_mysql();
void close_mysql(MYSQL *con);
json_object * query_mysql(json_object *jobj, MYSQL *con);
/* Write "n" bytes to a descriptor. */
ssize_t writen(int fd, const void *vptr, size_t n);
ssize_t readline(int fd, void *vptr, size_t n);

/* Fields name in messages */
char key[][32] = { 
   "mov_id",
   "color",
   "director_name",
   "num_critic_for_reviews",
   "duration",
   "actor_2_name",
   "genres",
   "actor_1_name",
   "movie_title",
   "cast_total_facebook_likes",
   "actor_3_name",
   "plot_keywords",
   "movie_imdb_link",
   "num_user_for_reviews",
   "language",
   "country",
   "content_rating",
   "title_year",
   "imdb_score",
   "aspect_ratio",
   "movie_facebook_likes",
   "start_no",
   "total_num"
};

// Main function
int main( int argc, char *argv[] ) {
   int sockfd, newsockfd, clilen;
   struct sockaddr_in serv_addr, cli_addr;
   int pid;

   signal(SIGCHLD, childDeath);

   /* First call to socket() function */
   sockfd = socket(AF_INET, SOCK_STREAM, 0);
   
   if (sockfd < 0) {
      perror("ERROR opening socket");
      exit(1);
   }
   
   /* Initialize socket structure */
   bzero((char *) &serv_addr, sizeof(serv_addr));
   
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_addr.s_addr = INADDR_ANY;
   serv_addr.sin_port = htons(PORTNO);
   
   /* Now bind the host address using bind() call.*/
   if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
      perror("ERROR on binding");
      exit(1);
   }
   
   /* Now start listening for the clients, here
      * process will go in sleep mode and will wait
      * for the incoming connection
   */
   
   listen(sockfd,5);
   clilen = sizeof(cli_addr);
  
   /* Main loop to wait for clients' connection */  
   while (1) {
      newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, (socklen_t *)&clilen);
		
      if (newsockfd < 0) {
         perror("ERROR on accept");
         exit(1);
      }
     
      printf("Connect to a client.\n");
 
      /* Create child process */
      pid = fork();
		
      if (pid < 0) {
         perror("ERROR on fork");
         exit(1);
      }
      
      if (pid == 0) {
         /* This is the client process */
         close(sockfd);
         doprocessing(newsockfd);
         exit(0);
      }
      else {
         printf("Create a child process %d.\n", pid);
         close(newsockfd);
      }
		
   } /* end of while */
}

/*
 * Function: Child's process
 * Input:   sock descriptor
 * Output:  NULL
 */
void doprocessing (int sock) {
   long len;
   char buffer[BUFSIZE];
   MYSQL *con;

   /* Set up connection to mysql server */
   con = setup_mysql();
   if(con == NULL) {
      printf("%d: Failed to connect to mysql, terminate ...\n", getpid());
      close(sock);
      exit(1);
   }

   /* Main loop of child's process */
   while (1) {
      /* Read message from client */
      bzero(buffer,BUFSIZE);
      len = readline(sock,buffer,BUFSIZE);
   
      if (len == 0) {
         printf("%d: Disconnected by client\n", getpid());
         break;
      }else if (len < 0) {
         perror("ERROR reading from socket");
         break;
      }
      printf("%d: Received message(len=%ld): %s\n", getpid(), len, buffer);

      /* Receive "quit" from client, terminate the corresponding child's process */
      if(strncmp(buffer, "quit", 4) == 0) {
         printf("%d: quit...\n", getpid());
         break;
      }

      /* Handle the received message stored in buffer and produce the reply
       *  message to buffer*/
      len = messageHandler(buffer, len, con);

      /* Add '\n' to the end of the message and send it to client */
      buffer[len++] = '\n';
printf("Sending: len=%ld, %s", len, buffer);
      len = writen(sock, buffer, len);
      if (len < 0) {
         perror("ERROR writing to socket");
         break;
      }
      printf("%d: Sent message with length=%ld\n", getpid(), len);
   }

   /* Close mysql connection */
   close_mysql(con);
   /* Close sock connection */
   close(sock);
   exit(0);
}

/* Signal handler of child's termination */
void childDeath(){
  pid_t pid;
  int status;
  pid = wait(&status);
  printf("Child %d has terminated\n", pid);
}

/*
 * Function: Handle the received message and produce the reply message
 * Input:   buffer - received message
 *          len - length of the received message
 *          con - mysql connection
 * Output:  buffer - reply message
 *          return - length of the reply message
 */
int messageHandler(char *buffer, int len, MYSQL *con) {
   json_object *jobj = NULL;
   json_object *jobjRes = NULL;
   const char *strTmp;

   /* parse the received message to json object */
   jobj = json_tokener_parse(buffer);
   /* Get mysql result */
   jobjRes = query_mysql(jobj, con);
   if(jobjRes == NULL) {
      /* If result is empty, reply the message with start_no=0 and totalnum=0 */
      jobjRes = json_object_new_object();
      json_object_object_add(jobjRes, key[STARTNO], json_object_new_int(0));
      json_object_object_add(jobjRes, key[TOTALNUM], json_object_new_int(0));
   }

   /* write the results to sending buffer */
   bzero(buffer,BUFSIZE);
   strTmp = json_object_to_json_string(jobjRes);
   strcpy(buffer, strTmp);
   /* Free the json objects */
   json_object_put(jobj);
   json_object_put(jobjRes);

   return strlen(buffer); 
}

/* Write "n" bytes to a descriptor. */
ssize_t writen(int fd, const void *vptr, size_t n)
{
    size_t nleft;
    ssize_t nwritten;
    const char *ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0) {
        if ( (nwritten = write(fd, ptr, nleft)) <= 0) {
            if (nwritten < 0 && errno == EINTR)
                nwritten = 0;   /* and call write() again */
            else
                return (-1);    /* error */
         }

         nleft -= nwritten;
         ptr += nwritten;
    }
    return (n);
}

/* Read a new line from a descriptor up to n bytes. */
ssize_t readline(int fd, void *vptr, size_t n)
{
    size_t  nleft;
    ssize_t nread;
    char   *ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0) {
        if ( (nread = read(fd, ptr, nleft)) < 0) {
            if (errno == EINTR)
                nread = 0;      /* and call read() again */
            else
                return (-1);
        } else if (nread == 0)
            break;              /* EOF */

        nleft -= nread;
        ptr += nread;

        /* '\n' is the end of a line.
           The read is complete.*/
        if(*(ptr-1) == '\n') break;
    }
    return (n - nleft);         /* return >= 0 */
}
