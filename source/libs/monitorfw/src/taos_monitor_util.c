/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */


#define ALLOW_FORBID_FUNC
#include "tjson.h"
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "osMemory.h"

#include "taos_metric_t.h"

void taos_monitor_split_str(char** arr, char* str, const char* del) {
  char *lasts;
  char* s = strsep(&str, del);
  while (s != NULL) {
    *arr++ = s;
    s = strsep(&str, del);
  }
}

void taos_monitor_split_str_metric(char** arr, taos_metric_t* metric, const char* del, char** buf) {
  int32_t size = strlen(metric->name);
  char* name = taosMemoryMalloc(size + 1);
  memset(name, 0, size + 1);
  memcpy(name, metric->name, size);

  char* s = strtok(name, del);
  while (s != NULL) {
    *arr++ = s;
    s = strtok(NULL, del);
  }

  *buf = name;
}

const char* taos_monitor_get_metric_name(taos_metric_t* metric){
  return metric->name;
}

int countOccurrences(char *str, char *toSearch) {
    int count = 0;
    char *ptr = str;
    while ((ptr = strstr(ptr, toSearch)) != NULL) {
        count++;
        ptr++;
    }
    return count;
}

void strip(char *s)
{
    size_t i;
    size_t len = strlen(s);
    size_t offset = 0;
    for(i = 0; i < len; ++i){
        char c = s[i];
        if(c=='\"') ++offset;
        else s[i-offset] = c;
    }
    s[len-offset] = '\0';
}

bool taos_monitor_is_match(const SJson* tags, char** pairs, int32_t count) { //todo
  int32_t size = tjsonGetArraySize(tags);
  if(size != count) return false;

  for(int32_t i = 0; i < size; i++){
    SJson* item = tjsonGetArrayItem(tags, i);

    bool isfound = true;
    for(int32_t j = 0; j < count; j++){

      char** pair = pairs + j * 2;

      char* key = *pair;
      char* value = *(pair + 1);

      char tmp[50] = {0}; //todo
      int32_t code = tjsonGetStringValue(item, key, tmp);
      if(code == 0 && strcmp(value, tmp) != 0){
        isfound = false;
        break;
      }
    }

    if(!isfound) return false;
  }

  return true;
}
