#include "sdb.h"
#include "mndTrans.h"

int clearType = SDB_TRANS;

int main(int argc, char** argv){
  if(argc == 1){
    printf("usage: clearTrans datadir(like /var/lib/taos/mnode)\n");
    return -1;
  }
  if(argc == 3){
    clearType = atoi(argv[2]);
  }

  int32_t code = 0;
  SMnode *pMnode = NULL;
  printf("datadir:%s, clear type:%s\n", argv[1], sdbTableName(clearType));

  SMnodeOpt opt2 = {0};
  pMnode = mndOpen(argv[1], &opt2);
  if(pMnode == NULL){
    printf("pMnode is null\n");
    goto END;
  }

END:
  mndClose(pMnode);
  return code;
}
