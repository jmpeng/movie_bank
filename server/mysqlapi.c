/*
 *  Author:     Group 6
 *  Date:       March 13, 2017
 *  Purpose:    mysql api to process the message
 */

#include <my_global.h>
#include <mysql.h>
#include <json.h>
#include <unistd.h>

#include "../inc/movie.h"

extern char key[][32];

/* Deal with the error situation */
void finish_with_error(MYSQL *con)
{
  fprintf(stderr, "%d: %s\n", getpid(), mysql_error(con));
  mysql_close(con);
  return;
}

/*
 * Function: set up connection to mysql server
 * Input:    NULL
 * Output:   connection desciptor
 */
MYSQL *setup_mysql()
{      
   MYSQL *con = mysql_init(NULL);
  
   if (con == NULL)
   {
      fprintf(stderr, "%d: mysql_init() failed\n", getpid());
      return NULL;
   }  
  
   if (mysql_real_connect(con, "localhost", "jmpeng", "654321", 
          "movie_db", 0, NULL, 0) == NULL) 
   {
      finish_with_error(con);
      return NULL;
   }

   return con;
}

/* Close connection to mysql server */ 
void close_mysql(MYSQL *con) {
   mysql_close(con);
   return;
}

/*
 * Function:    Select mysql according requested message and 
 *              construct the reply message
 * Input:       jobj - requested message in json format
 *              con - mysql connection
 * Output:      reply message in json format
 *
 */
json_object * query_mysql(json_object *jobj, MYSQL *con) {
   json_object *jobjRes = NULL;
   int start_no, total_num;
   json_object *jobjtmp;
   int val_type;
   char whereClause[1024];
   char selectClause[1024];
   char * pstr;
   const char * pstr1;
   MYSQL_RES *result;
   char fieldName[48][128];
   char strTmp[512];
   json_object *jobjArray;
   MYSQL_FIELD *field;
   MYSQL_ROW row;
   json_object *jobjRow = NULL;

   total_num = 0;
   start_no = 0;

   if (jobj == NULL) return NULL;

   whereClause[0] = '\0';
   selectClause[0] = '\0';

   strcpy(whereClause, "where ");

   /* Extract the start_no */
   json_object_object_get_ex(jobj, key[STARTNO], &jobjtmp);
   if(jobjtmp != NULL) {
      val_type = json_object_get_type(jobjtmp);
      if(val_type == json_type_int) {
         start_no = json_object_get_int(jobjtmp);
      }
      json_object_put(jobjtmp);
      jobjtmp = NULL;
      json_object_object_del(jobj, key[STARTNO]);
   }
 
   /* Extract all the conditions for searching in mysql */ 
   json_object_object_foreach(jobj, key1, val) {
      val_type = json_object_get_type(val);
      if(val_type == json_type_int) {
         pstr = whereClause + strlen(whereClause);
         sprintf(pstr, "%s>=%d and ", key1, json_object_get_int(val));
      } else if (val_type == json_type_string) {
         pstr1 = json_object_get_string(val);
         pstr = whereClause + strlen(whereClause);
         sprintf(pstr, "%s like \'%%%s%%\' and ", key1, pstr1);
      } else {
         printf("%d: Wrong type of key: %s\n", getpid(), key1);
      }
   }
  
   if(strlen(whereClause) > strlen("where and ")) { 
      pstr = whereClause + strlen(whereClause)-4;
      if(!strncmp(pstr, "and ", 4)) {
         /* Remove the last "and " in the end of the string */
         whereClause[strlen(whereClause)-4] = '\0';
      }
   } else 
      whereClause[0] = '\0';  /* No condition of where, remove "where" */

   /* Construct the select command for mysql to search all the matched records in order to get the total number */
   sprintf(selectClause, "SELECT * FROM movie %s ORDER BY title_year DESC, imdb_score DESC;", whereClause);
   fprintf(stdout, "%d: %s\n", getpid(), selectClause);

  if (mysql_query(con, selectClause))
  {
      finish_with_error(con);
      return NULL;
  }

  /* Get the results */ 
  result = mysql_store_result(con);
 
  if (result == NULL)
  {
      finish_with_error(con);
      return NULL;
  }
   /* Get the total number of mached records */
   total_num = mysql_num_rows(result);
   mysql_free_result(result);
   result = NULL;

   printf("%d: total_num=%d\n", getpid(), total_num); 
   if (total_num == 0) return NULL;

   /* Construct the select command for mysql to search 10 records starting from start_no */
   sprintf(selectClause, "SELECT * FROM movie %s ORDER BY title_year DESC, imdb_score DESC limit %d,10;", whereClause, start_no);
   fprintf(stdout, "%d: %s\n", getpid(), selectClause);
 
  if (mysql_query(con, selectClause)) 
  {
      finish_with_error(con);
      return NULL;
  }
 
  /* Get the results */ 
  result = mysql_store_result(con);
  
  if (result == NULL) 
  {
      finish_with_error(con);
      return NULL;
  }

   /* Get all the field name of the records */
   int i = 0;
   while((field = mysql_fetch_field(result)))
   {
      fieldName[i][0] = '\0';
      strcpy(fieldName[i], field->name);
      i++;
   }

  int num_fields = mysql_num_fields(result);

  /* Add all the filed values to json object */ 
  jobjArray = json_object_new_array();
  while ((row = mysql_fetch_row(result))) 
  { 
      jobjRow = json_object_new_object();
      for(int i = 0; i < num_fields; i++) 
      {
         strTmp[0] = '\0'; 
         sprintf(strTmp, "%s ", row[i] ? row[i] : "NULL"); 
         json_object_object_add(jobjRow, fieldName[i], json_object_new_string(strTmp));
      } 
      json_object_array_add(jobjArray, jobjRow);
      jobjRow = NULL;
  }

  /* Construct the final json object for reply message */
  jobjRes = json_object_new_object();
  json_object_object_add(jobjRes, key[STARTNO], json_object_new_int(start_no));
  json_object_object_add(jobjRes, key[TOTALNUM], json_object_new_int(total_num));
  json_object_object_add(jobjRes, "movies", jobjArray);
  
  mysql_free_result(result);
  
  return jobjRes;
}

