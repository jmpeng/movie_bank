/*
 *  Author:     Group 6
 *  Date:       March 13, 2017
 *  Purpose:    Client to search movie information
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>

#include <netdb.h>
#include <netinet/in.h>

#include <json.h>
#include <string.h>
#include "../inc/movie.h"

int getInput(char *buffer);
int getContinueInput(char *buffer);
/* Write "n" bytes to a descriptor. */
ssize_t writen(int fd, const void* ptr, size_t n);
/* Read "n" bytes from a descriptor. */
ssize_t readline(int fd, void *ptr, size_t n);
int messageHandler(char *inBuffer, long inDataLen);
void disp_verbose(json_object *jobjrec);
void disp_list(json_object *jarray);
void disp_list_one(json_object *jobjrec);

int isQuit = 0;
// json object to store user's searching criteria
json_object *jobj = NULL;
// Search from curCount record. If it is 0, which means a new searching
int curCount = 0;
int totalCount = 0;

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

/* Main function */
int main(int argc, char *argv[]) {
   int sockfd, portno;
   struct sockaddr_in serv_addr;
   struct hostent *server;

   // Buffer for read data from server   
   char inBuffer[BUFSIZE];
   // Buffer for write data to server   
   char outBuffer[BUFSIZE];
   // Data Length of data read/write from/to server
   long inDataLen, outDataLen, len;
   
   if (argc < 3) {
      fprintf(stderr,"usage %s hostname port\n", argv[0]);
      exit(0);
   }
	
   portno = atoi(argv[2]);
   
   /* Create a socket point */
   sockfd = socket(AF_INET, SOCK_STREAM, 0);
   
   if (sockfd < 0) {
      perror("ERROR opening socket");
      exit(1);
   }
	
   server = gethostbyname(argv[1]);
   
   if (server == NULL) {
      fprintf(stderr,"ERROR, no such host\n");
      exit(0);
   }
   
   bzero((char *) &serv_addr, sizeof(serv_addr));
   serv_addr.sin_family = AF_INET;
   bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
   serv_addr.sin_port = htons(portno);
   
   printf("Start to connect to server %s with port %d\n",argv[1], portno );

   /* Now connect to the server */
   if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
      perror("ERROR connecting");
      exit(1);
   }
   
   /* Now ask for a message from the user, this message
    * will be read by server
    */
   while(1) {
      bzero(inBuffer,BUFSIZE);
      bzero(outBuffer,BUFSIZE);

      if(curCount == 0) {
         // Start a new searching
         outDataLen = getInput(outBuffer);
      } else {
         // Not a new searching.
         // Ask user for countinuing
         outDataLen = getContinueInput(outBuffer);
      }
      if( (outDataLen <= 0) || isQuit ) {
         /* Send quit message to the server */
         len = writen(sockfd, "quit\n", 5);
         break;
      } 
   
      /* Send message to the server */
      outBuffer[outDataLen++] = '\n';
      len = writen(sockfd, outBuffer, outDataLen);
   
      if (len < 0) {
         perror("ERROR writing to socket");
         exit(1);
      }
   
      /* Now read server response */
      inDataLen = readline(sockfd, inBuffer, BUFSIZE-1);
      if (inDataLen < 0) {
         perror("ERROR reading from socket");
         exit(1);
      }

      /* Handle the received message */
      messageHandler(inBuffer, inDataLen);	
   }

   /* Free the json object */
   if(jobj != NULL) json_object_put(jobj);

   close(sockfd);
   return 0;
}

/*
 * Function:   Get user's input about the condition to search movies
 * Input:      buffer - memory buffer to store the constructed request message in json format
 * Output:     length of the request message
 */
int getInput(char *buffer) {
   char inStr[256];
   const char *strTmp;
   int year=0;

   /* Loop for user to input movie searching criterias. */
   while(1) {
      memset(inStr, '\0', sizeof(inStr));
      if(jobj != NULL)
         json_object_put(jobj);

      jobj = json_object_new_object();

      printf("Search movies according to the following criteria.\n");

      // Movie title
      printf("\nMovie title:");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(strlen(inStr)) {
         json_object_object_add(jobj, key[MOV_TITLE], json_object_new_string(inStr));
      }

      // Genres
      memset(inStr, '\0', sizeof(inStr));
      printf("\nGenres(Adventure|Animation|Comedy|Family|Fantasy|Musical|Romance|Action|Sci-Fi|Mystery|Drama):");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(strlen(inStr)) {
         json_object_object_add(jobj, key[MOV_GENRES], json_object_new_string(inStr));
      }

      // Actor/Actress 1
      memset(inStr, '\0', sizeof(inStr));
      printf("\nActor/Actress 1:");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(strlen(inStr)) {
         json_object_object_add(jobj, key[MOV_ACTOR1], json_object_new_string(inStr));
      }

      // Actor/Actress 2
      memset(inStr, '\0', sizeof(inStr));
      printf("\nActor/Actress 2:");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(strlen(inStr)) {
         json_object_object_add(jobj, key[MOV_ACTOR2], json_object_new_string(inStr));
      }

      // Actor/Actress 3
      memset(inStr, '\0', sizeof(inStr));
      printf("\nActor/Actress 3:");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(strlen(inStr)) {
         json_object_object_add(jobj, key[MOV_ACTOR3], json_object_new_string(inStr));
      }

      // Director
      memset(inStr, '\0', sizeof(inStr));
      printf("\nDirector:");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(strlen(inStr)) {
         json_object_object_add(jobj, key[MOV_DIRECTOR], json_object_new_string(inStr));
      }

      // year
      printf("\nFrom year:");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      year = atoi(inStr);
      if(year > 0) {
         json_object_object_add(jobj, key[MOV_YEAR], json_object_new_int(year));
      }

      // Country
      memset(inStr, '\0', sizeof(inStr));
      printf("\nCountry:");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(strlen(inStr)) {
         json_object_object_add(jobj, key[MOV_COUNTRY], json_object_new_string(inStr));
      }
  
      // Write json format message to buffer for sending message
      strTmp = json_object_to_json_string(jobj);
      strcpy(buffer, strTmp);

      // Search movie 
      memset(inStr, '\0', sizeof(inStr));
      printf("\nSearch movies?(Y/N):");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(!strcmp(inStr, "Y") || !strcmp(inStr, "y")) {
         // "Y", search movies
         printf("Searching movies ......\n");
         return strlen(buffer);
      }
   
      // Quit or search again
      memset(inStr, '\0', sizeof(inStr));
      printf("\nQuit?(Y/N):");
      fgets(inStr,255,stdin);
      inStr[strlen(inStr)-1] = '\0';
      if(!strcmp(inStr, "Y") || !strcmp(inStr, "y")) {
         // Quit the program
         isQuit = 1;
         return 0;
      }
   }
}


/*
 * Function:   Ask user to continue the current seaching or start a new searching
 * Input:      buffer - memory buffer to store the constructed request message in json format
 * Output:     length of the request message
 */
int getContinueInput(char *buffer) {
   char inStr[256];
   // Number of records left
   int leftCount = totalCount - curCount - 1;
   const char * strTmp;

   if(leftCount <= 0) {
      // Start a new searching
      json_object_put(jobj);
      jobj = NULL;
      curCount = 0;
      totalCount = 0;
      return getInput(buffer);
   }

   printf("\nThere are %d matched move records left. Do you want to see more?(Y/N):", leftCount); 
   memset(inStr, '\0', sizeof(inStr));
   fgets(inStr,255,stdin);
   inStr[strlen(inStr)-1] = '\0';
   if(!strcmp(inStr, "Y") || !strcmp(inStr, "y")) {
      // "Y", search more movies
      if(jobj == NULL) {
         printf("Error: jobj is NULL when continuing the search. Quit...\n");
         isQuit = 1;
         return 0;
      }

      /* 
       * Replace the start_no with the new one in the previous json object which stored
       * the previous request message. Because the search criteria is not changed, use
       * the same json object.
       */
      json_object_object_del(jobj, key[STARTNO]);
      json_object_object_add(jobj, key[STARTNO], json_object_new_int(curCount));
      strTmp = json_object_to_json_string(jobj);
      strcpy(buffer, strTmp);
      printf("Searching movies ......\n");
      return strlen(buffer);
   }
  
   // Quit or search again
   memset(inStr, '\0', sizeof(inStr));
   printf("\nQuit?(Y/N):");
   fgets(inStr,255,stdin);
   inStr[strlen(inStr)-1] = '\0';
   if(!strcmp(inStr, "Y") || !strcmp(inStr, "y")) {
      // Quit the program
      isQuit = 1;
      return 0;
   }

   // Start a new searching
   json_object_put(jobj);
   jobj = NULL;
   curCount = 0;
   totalCount = 0;
   return getInput(buffer);
}

/* Read a new line from a descriptor up to n bytes. */
 ssize_t readline(int fd, void *ptr, size_t n)  
 {  
  size_t nleft;  
  ssize_t nread;  
   
  nleft = n;  
  while(nleft > 0) {  
   if((nread = read(fd, ptr, nleft)) < 0) {  
    if(nleft == n)  
     return -1;  
    else  
     break;  
   } else if(nread == 0) {  
    break;  
   }  
   nleft -= nread;  
   ptr += nread;

   // '\n' is the end of a line.
   // The read is complete.
   if(*(char *)(ptr-1) == '\n') break;
  }  
   
  return n-nleft;  
 }  
 
/* Write "n" bytes to a descriptor. */ 
 ssize_t writen(int fd, const void* ptr, size_t n)  
 {  
  size_t nleft;  
  ssize_t nwritten;  
   
  nleft = n;  
  while(nleft > 0) {  
   if((nwritten = write(fd, ptr, nleft)) < 0) {  
    if(nleft == n)  
     return -1;  
    else  
     break;  
   } else if (nwritten == 0) {  
    break;  
   }  
   
   nleft -= nwritten;  
   ptr += nwritten;  
  }  
   
  return n-nleft;  
}  

/*
 * Function:   Handle message received from server
 * Input:      inBuffer - Store the received message
 *             inDataLen - Length of the message
 * Output:     0 - successful
 *             !0 - Failed           
 */   
int messageHandler(char *inBuffer, long inDataLen) {
   // json object to store message received from server
   json_object *jobjreply = NULL;
   json_object *jarray;
   json_object *jobjtmp = NULL;
   int val_type;
   int n;
   int arrayLen;

   /* Parse message to json object */
   jobjreply = json_tokener_parse(inBuffer);

   /* Get the start_no */
   json_object_object_get_ex(jobjreply, key[STARTNO], &jobjtmp);
   if(jobjtmp != NULL) {
      val_type = json_object_get_type(jobjtmp);
      if(val_type == json_type_int) {
         n = json_object_get_int(jobjtmp);
         if(n != curCount)
            printf("Error: start no of records %d is not the same with requested %d.\n", n, curCount);
      }
      json_object_put(jobjtmp);
      jobjtmp = NULL;
   }

   /* Get the total number */
   json_object_object_get_ex(jobjreply, key[TOTALNUM], &jobjtmp);
   if(jobjtmp != NULL) {
      val_type = json_object_get_type(jobjtmp);
      if(val_type == json_type_int) {
         totalCount = json_object_get_int(jobjtmp);
      }
      json_object_put(jobjtmp);
      jobjtmp = NULL;
   }

   /* If the total number is 0, it means it is an empty reply without matched records.
    * Start a new searching */
   if(totalCount == 0) {
      // Search results are empty, reset the searching
      curCount = 0;
      json_object_put(jobjreply);
      json_object_put(jobj);
      return 0;
   }

   /* Get the array in json object which contains all the matched movie records */
   jarray = json_object_object_get(jobjreply, "movies");
   /*Getting the length of the array*/
   arrayLen = json_object_array_length(jarray);
   if(curCount + arrayLen > totalCount) {
      printf("Error: start no(%d) + number of records(%d) > total number of records(%d)\n",
         curCount, arrayLen, totalCount);
   }

   /* Display all the matched movies */
   disp_list(jarray);

   curCount += arrayLen;
   json_object_put(jobjreply);
   return 0;
}

/*
 * Function: List all the matched movies
 * Input:   jarray - json object which contains all the matched  movie records
 * Output:  NULL
 */
void disp_list(json_object *jarray) {
   int num;
   int arrayLen;
   json_object *jobjtmp = NULL;
   char cha;

   printf("Start no: %d, total records: %d\n", curCount, totalCount);
   printf("Index  Movie_id                      Movie_title           Movie_year      Movie_genres       Movie_score     Movie_country\n");
   printf("===========================================================================================================================\n");

   /* List all the moives in a table format */
   arrayLen = json_object_array_length(jarray);
   for (int i=0; i< arrayLen; i++){
      printf("%5d", i+curCount);
      jobjtmp = json_object_array_get_idx(jarray, i); /*Getting the array element at position i*/
      disp_list_one(jobjtmp);
   }
   while(1) {
      printf("===========================================================================================================================\n");
     /* Get a number from user to display a specific movie */
      printf("Choose the index from the list(%d - %d) to see details(-1 to skip)", curCount, curCount + arrayLen - 1);
      /* num - get the input number; cha - get the '\n' */
      scanf("%d%c", &num, &cha);
      if(num == -1) break;
      num -= curCount;
      if(num <0 || num >= arrayLen) {
         printf("Wrong input, try again.\n");
         continue;
      }
      /* Display the details of the chosen movie */
      jobjtmp = json_object_array_get_idx(jarray, num); /*Getting the array element at position i*/
      disp_verbose(jobjtmp);
   }
   return;
}

/*
 * Function:   List a movie 
 * Input:      json object of the movie
 * Output:     NULL
 */
void disp_list_one(json_object *jobjrec) {
   json_object *jobjtmp = NULL;
   int mov_id;
   char mov_title[256];
   int mov_year;
   int mov_score;
   char mov_genres[128];
   const char *mov_country;


   // Extract Movie_id
   json_object_object_get_ex(jobjrec, key[MOV_ID], &jobjtmp);
   mov_id = json_object_get_int(jobjtmp);
//   json_object_put(jobjtmp);

   // Extract Movie_title
   json_object_object_get_ex(jobjrec, key[MOV_TITLE], &jobjtmp);
   sprintf(mov_title,"%s", json_object_get_string(jobjtmp));
   //json_object_put(jobjtmp);

   // Extract Movie_year
   json_object_object_get_ex(jobjrec, key[MOV_YEAR], &jobjtmp);
   mov_year = json_object_get_int(jobjtmp);
   //json_object_put(jobjtmp);

   // Extract Movie_genres
   json_object_object_get_ex(jobjrec, key[MOV_GENRES], &jobjtmp);
   sprintf(mov_genres, "%s", json_object_get_string(jobjtmp));
   //json_object_put(jobjtmp);

   // Extract Movie_score
   json_object_object_get_ex(jobjrec, key[MOV_SCORE], &jobjtmp);
   mov_score = json_object_get_int(jobjtmp);
   //json_object_put(jobjtmp);

   // Extract Movie_country
   json_object_object_get_ex(jobjrec, key[MOV_COUNTRY], &jobjtmp);
   mov_country = json_object_get_string(jobjtmp);
   //json_object_put(jobjtmp);

   mov_title[40] = '\0';
   mov_genres[20] = '\0';
   printf("  %6d    %40s    %4d       %20s    %2.1d                %6s\n",mov_id, mov_title, mov_year, mov_genres, mov_score, mov_country);
   return;
}

/*
 * Function:   Display the details of a movie
 * Input:      json object of the movie
 * Output:     NULL
 */
void disp_verbose(json_object *jobjrec) {
   int val_type;
   char *pstr1;

   /* Display all the fields as well as values. */
   json_object_object_foreach(jobjrec, key1, val) {
      printf("%s: ", key1);
      val_type = json_object_get_type(val);
      if(val_type == json_type_int) {
         printf("%d\n", json_object_get_int(val));
      } else if (val_type == json_type_string) {
         pstr1 = (char *) json_object_get_string(val);
         printf("%s\n", pstr1);
      } else {
         printf("Wrong type %d of key: %s\n", val_type, key1);
      }
   }
   return;
}
