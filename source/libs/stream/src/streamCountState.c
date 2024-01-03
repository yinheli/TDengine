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

#include "tstreamFileState.h"

#include "query.h"
#include "streamBackendRocksdb.h"
#include "taos.h"
#include "tcommon.h"
#include "thash.h"
#include "tsimplehash.h"

// todo(liuyao) 缓存用map来存，key是group id, value是array，array的Item按ts, uid排序（先按ts，ts相同再按uid)
// todo(liuyao) 生成check point后，cache会和rocksdb有一部分是重合的，这时以cache为准。rocksdb遍历时，只遍历到flush mark

int countWinKeyCmpr(const SCountWinKey* pWin1, const SCountWinKey* pWin2) {
  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->win.skey > pWin2->win.skey) {
    return 1;
  } else if (pWin1->win.skey < pWin2->win.skey) {
    return -1;
  }

  if (pWin1->win.ekey > pWin2->win.ekey) {
    return 1;
  } else if (pWin1->win.ekey < pWin2->win.ekey) {
    return -1;
  }

  if (pWin1->startWinUid > pWin2->startWinUid) {
    return 1;
  } else if (pWin1->startWinUid < pWin2->startWinUid) {
    return -1;
  }

  if (pWin1->endWinUid > pWin2->endWinUid) {
    ASSERT(0);
    return 1;
  } else if (pWin1->endWinUid < pWin2->endWinUid) {
    ASSERT(0);
    return -1;
  }

  return 0;
}

int countStateKeyCompare(const SCountWinKey* pWin1, const void* pDatas, int pos) {
  SRowBuffPos* pPos2 = taosArrayGetP(pDatas, pos);
  SCountWinKey* pWin2 = (SCountWinKey*) pPos2->pKey;
  return countWinKeyCmpr(pWin1, pWin2);
}

static void checkAndTransformCountCursor(SStreamFileState* pFileState, const uint64_t groupId, SArray* pWinStates, SStreamStateCur** ppCur) {
  SCountWinKey key = {.groupId = groupId};
  int32_t code = streamStateCountGetKVByCur_rocksdb(*ppCur, &key, NULL, NULL);
  if (taosArrayGetSize(pWinStates) > 0 && (code == TSDB_CODE_FAILED || countStateKeyCompare(&key, pWinStates, 0) >= 0)) {
    if ( !(*ppCur) ) {
      (*ppCur) = createStreamStateCursor();
    }
    transformCursor(pFileState, *ppCur);
  } else if (*ppCur) {
    (*ppCur)->buffIndex = -1;
    (*ppCur)->pStreamFileState = pFileState;
  }
}

static SStreamStateCur* seekCountKeyCurrentPrev_buff(SStreamFileState* pFileState, const SCountWinKey* pWinKey,
                                                SArray** pWins, int32_t* pIndex) {
  SStreamStateCur* pCur = NULL;
  SSHashObj*       pSessionBuff = getRowStateBuff(pFileState);
  void**           ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (!ppBuff) {
    return NULL;
  }

  SArray* pWinStates = (SArray*)(*ppBuff);
  int32_t size = taosArrayGetSize(pWinStates);
  TSKEY   gap = 0;
  int32_t index = binarySearch(pWinStates, size, pWinKey, countStateKeyCompare);

  if (pWins) {
    (*pWins) = pWinStates;
  }

  if (index >= 0) {
    pCur = createStreamStateCursor();
    pCur->buffIndex = index;
    pCur->pStreamFileState = pFileState;
    if (pIndex) {
      *pIndex = index;
    }
  }
  return pCur;
}

SStreamStateCur* countWinStateSeekKeyCurrentNext(SStreamFileState* pFileState, const SCountWinKey* pWinKey) {
  SArray* pWinStates = NULL;
  int32_t index = -1;
  SStreamStateCur* pCur = seekCountKeyCurrentPrev_buff(pFileState, pWinKey, &pWinStates, &index);
  if (pCur) {
    if (countStateKeyCompare(pWinKey, pWinStates, index) > 0) {
      sessionWinStateMoveToNext(pCur);
    }
    return pCur;
  }

  void* pFileStore = getStateFileStore(pFileState);
  pCur = streamStateCountSeekKeyCurrentNext_rocksdb(pFileStore, (SCountWinKey*)pWinKey);
  checkAndTransformCountCursor(pFileState, pWinKey->groupId, pWinStates, &pCur);
  return pCur;
}

int32_t countWinStateGetKVByCur(SStreamStateCur* pCur, SCountWinKey* pKey, void** pVal, int32_t* pVLen) {
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }
  int32_t code = TSDB_CODE_SUCCESS;

  SSHashObj* pSessionBuff = getRowStateBuff(pCur->pStreamFileState);
  void**     ppBuff = tSimpleHashGet(pSessionBuff, &pKey->groupId, sizeof(uint64_t));
  if (!ppBuff) {
    return TSDB_CODE_FAILED;
  }

  SArray* pWinStates = (SArray*)(*ppBuff);
  int32_t size = taosArrayGetSize(pWinStates);
  if (pCur->buffIndex >= 0) {
    if (pCur->buffIndex >= size) {
      return TSDB_CODE_FAILED;
    }
    SRowBuffPos* pPos = taosArrayGetP(pWinStates, pCur->buffIndex);
    if (pVal) {
      *pVal = pPos;
    }
    *pKey = *(SCountWinKey*)(pPos->pKey);
  } else {
    void* pData = NULL;
    code = streamStateCountGetKVByCur_rocksdb(pCur, pKey, &pData, pVLen);
    if (taosArrayGetSize(pWinStates) > 0 && (code == TSDB_CODE_FAILED || countStateKeyCompare(pKey, pWinStates, 0) >= 0)) {
      transformCursor(pCur->pStreamFileState, pCur);
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, pCur->buffIndex);
      if (pVal) {
        *pVal = pPos;
      }
      *pKey = *(SCountWinKey*)(pPos->pKey);
      code = TSDB_CODE_SUCCESS;
    } else if (code == TSDB_CODE_SUCCESS && pVal) {
      SRowBuffPos* pNewPos = getNewRowPosForWrite(pCur->pStreamFileState);
      memcpy(pNewPos->pKey, pKey, sizeof(SCountWinKey));
      pNewPos->needFree = true;
      memcpy(pNewPos->pRowBuff, pData, *pVLen);
      (*pVal) = pNewPos;
    }
    taosMemoryFreeClear(pData);
  }
  return code;
}
