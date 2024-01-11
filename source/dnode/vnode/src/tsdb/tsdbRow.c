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

#include "tsdbRow.h"

void tsdbRowGetKey(TSDBROW *row, STRowKey *rowKey) {
  if (row->type == TSDBROW_ROW_FMT) {
    tRowGetKey(row->pTSRow, &rowKey->rowKey);
    rowKey->version = row->version;
  } else if (row->type == TSDBROW_COL_FMT) {
    rowKey->rowKey.ts = row->pBlockData->aTSKEY[row->iRow];
    rowKey->version = row->pBlockData->aVersion[row->iRow];
    if (row->pBlockData->nColData == 0) {
      rowKey->rowKey.kType = TSDB_DATA_TYPE_NULL;
    } else {
      SColData *pColData = &row->pBlockData->aColData[0];
      if (pColData->smaOn /* TODO */ & COL_IS_KEY) {
        rowKey->rowKey.kType = pColData->type;
        SColVal cv;
        tColDataGetValue(pColData, row->iRow, &cv);
        rowKey->rowKey.value = cv.value;
      } else {
        rowKey->rowKey.kType = TSDB_DATA_TYPE_NULL;
      }
    }
  } else {
    ASSERT(0);
  }
}

int32_t tTRowKeyCmpr(const void *p1, const void *p2) {
  STRowKey *rowKey1 = (STRowKey *)p1;
  STRowKey *rowKey2 = (STRowKey *)p2;
  int32_t   ret = tRowKeyCmpr(&rowKey1->rowKey, &rowKey2->rowKey);

  if (ret == 0) {
    if (rowKey1->version < rowKey2->version) {
      return -1;
    } else if (rowKey1->version > rowKey2->version) {
      return 1;
    }
  }

  return 0;
}