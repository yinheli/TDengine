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
#include "executorInt.h"
#include "function.h"
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "tchecksum.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttime.h"

#define IS_FINAL_COUNT_OP(op)           ((op)->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_COUNT)
#define STREAM_COUNT_OP_STATE_NAME      "StreamCountHistoryState"
#define STREAM_COUNT_OP_CHECKPOINT_NAME "StreamCountOperator_Checkpoint"

typedef struct SCountWindowInfo {
  SResultWindowInfo winInfo;
  int32_t           windowCount;
  bool              lastWindow;
  uint64_t          startWinUid;
  uint64_t          endWinUid;
} SCountWindowInfo;

typedef struct SStreamCountAggSupporter {
  int32_t             resultRowSize;  // the result buffer size for each result row, with the meta data size for each row
  SSDataBlock*        pScanBlock;
  SStreamState*       pDataState;
  SStreamState*       pWinState;
  SSHashObj*          pResultRows;
  SDiskbasedBuf*      pResultBuf;
  SStateStore         stateStore;
  STimeWindow         winRange;
  SStorageAPI*        pSessionAPI;
  struct SUpdateInfo* pUpdateInfo;
  int32_t             windowCount;
} SStreamCountAggSupporter;

void destroyStreamCountOperatorInfo(void* param) {
  SStreamCountAggOperatorInfo* pInfo = (SStreamCountAggOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  destroyStreamAggSupporter(&pInfo->streamAggSup);
  clearGroupResInfo(&pInfo->groupResInfo);
  cleanupExprSupp(&pInfo->scalarSupp);

  colDataDestroy(&pInfo->twAggSup.timeWindowData);
  blockDataDestroy(pInfo->pDelRes);
  tSimpleHashCleanup(pInfo->pSeUpdated);
  tSimpleHashCleanup(pInfo->pSeDeleted);
  pInfo->pUpdated = taosArrayDestroy(pInfo->pUpdated);
  cleanupGroupResInfo(&pInfo->groupResInfo);

  taosArrayDestroy(pInfo->historyWins);
  blockDataDestroy(pInfo->pCheckpointRes);

  taosMemoryFreeClear(param);
}

void setCountOutputBuf(SStreamCountAggSupporter* pAggSup, TSKEY ts, uint64_t groupId, uint64_t uid, SCountWindowInfo* pCurWin,
                       bool* pRebuild) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t size = pAggSup->resultRowSize;
  void* pVal = NULL;
  int32_t len = 0;
  SCountWinKey key = {.win.skey = ts, .win.ekey = ts, .groupId = groupId, .startWinUid = uid, .endWinUid = uid};
  SCountWinKey tmpKey =  key;
  SCountWinOtherInfo other = {0};
  int32_t code = pAggSup->stateStore.streamStateCountWinAddIfNotExist(pAggSup->pWinState, &key, &pVal, &len, &other);
  if (code == TSDB_CODE_SUCCESS && inWinRange(&pAggSup->winRange, &key.win) ) {
    pCurWin->winInfo.sessionWin.win = key.win;
    pCurWin->winInfo.sessionWin.groupId = key.groupId;
    pCurWin->winInfo.pStatePos = pVal;
    pCurWin->windowCount = other.windowCount;
    pCurWin->lastWindow = other.lastWindow;
    pCurWin->startWinUid = key.startWinUid;
    pCurWin->endWinUid = key.endWinUid;
  }

  if (pCurWin->windowCount + 1 > pAggSup->windowCount) {
    if (pCurWin->lastWindow) {
      pVal = NULL;
      len = 0;
      code = pAggSup->stateStore.streamStateCountWinAdd(pAggSup->pWinState, &key, &pVal, &len);
      pCurWin->winInfo.sessionWin.win = key.win;
      pCurWin->winInfo.sessionWin.groupId = key.groupId;
      pCurWin->winInfo.pStatePos = pVal;
      pCurWin->startWinUid = key.startWinUid;
      pCurWin->endWinUid = key.endWinUid;
      pCurWin->windowCount = 0;
      pCurWin->lastWindow = true;
    } else {
      *pRebuild = true;
    }
  }
}

void transWinInfo2Key(const SCountWindowInfo* pWinInfo, SCountWinKey* pKey) {
  pKey->win = pWinInfo->winInfo.sessionWin.win;
  pKey->groupId = pWinInfo->winInfo.sessionWin.groupId;
  pKey->startWinUid = pWinInfo->startWinUid;
  pKey->endWinUid = pWinInfo->endWinUid;
}

bool doDeleteCountWindow(SStreamCountAggSupporter* pAggSup, SSHashObj* pSeUpdated, SCountWindowInfo* pWinInfo) {
  SCountWinKey key = {0};
  transWinInfo2Key(pWinInfo, &key);
  pAggSup->stateStore.streamStateCountDeleteWins(pAggSup->pWinState, &key);
  removeSessionResult(pAggSup, pSeUpdated, pAggSup->pResultRows, &key);
  return true;
}

int32_t updateCountWindowInfo(SCountWindowInfo* pWinInfo, TSKEY* pTs, int32_t start, int32_t rows, int32_t maxRows,
                              bool* pRebuild) {
  int32_t num = 0;
  for (int32_t i = start; i < rows; i++) {
    if (pTs[i] < pWinInfo->winInfo.sessionWin.win.ekey) {
      num++;
    } else {
      break;
    }
  }
  int32_t maxNum = TMIN(maxRows - pWinInfo->winCount, rows - start);
  if (num > maxNum) {
    *pRebuild = true;
  }
  return maxNum;
}

int32_t saveCountOutputBuf(SStreamCountAggSupporter* pAggSup, SResultWindowInfo* pWinInfo) {
  qDebug("===stream===try save count result skey:%" PRId64 ", ekey:%" PRId64 ".pos%d", pWinInfo->sessionWin.win.skey,
         pWinInfo->sessionWin.win.ekey, pWinInfo->pStatePos->needFree);
  return pAggSup->stateStore.streamStateSessionPut(pAggSup->pWinState, &pWinInfo->sessionWin, pWinInfo->pStatePos,
                                                   pAggSup->resultRowSize);
}

static bool doStreamCountAggImpl(SOperatorInfo* pOperator, SSDataBlock* pSDataBlock, SSHashObj* pSeUpdated,
                                 SSHashObj* pStDeleted, SCountWindowInfo* pCurWinInfo) {
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*                 pAPI = &pOperator->pTaskInfo->storageAPI;
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      numOfOutput = pOperator->exprSupp.numOfExprs;
  uint64_t                     groupId = pSDataBlock->info.id.groupId;
  uint64_t                     uid = pSDataBlock->info.id.uid;
  int64_t                      code = TSDB_CODE_SUCCESS;
  TSKEY*                       tsCols = NULL;
  SResultRow*                  pResult = NULL;
  int32_t                      winRows = 0;
  SStreamCountAggSupporter*    pAggSup = &pInfo->streamAggSup;
  bool                         rebuild = false;
  SCountWindowInfo             curWin = {0};

  pInfo->dataVersion = TMAX(pInfo->dataVersion, pSDataBlock->info.version);
  pAggSup->winRange = pTaskInfo->streamInfo.fillHistoryWindow;
  if (pAggSup->winRange.ekey <= 0) {
    pAggSup->winRange.ekey = INT64_MAX;
  }

  if (pSDataBlock->pDataBlock != NULL) {
    SColumnInfoData* pColDataInfo = taosArrayGet(pSDataBlock->pDataBlock, pInfo->primaryTsIndex);
    tsCols = (int64_t*)pColDataInfo->pData;
  } else {
    return;
  }

  int32_t rows = pSDataBlock->info.rows;
  blockDataEnsureCapacity(pAggSup->pScanBlock, rows);
  for (int32_t i = 0; i < rows; i += winRows) {
    if (pInfo->ignoreExpiredData && checkExpiredData(&pInfo->streamAggSup.stateStore, pInfo->streamAggSup.pUpdateInfo,
                                                     &pInfo->twAggSup, pSDataBlock->info.id.uid, tsCols[i])) {
      i++;
      continue;
    }
    curWin = (SCountWindowInfo){0};
    setCountOutputBuf(pAggSup, tsCols[i], groupId, uid, &curWin, &rebuild);
    setSessionWinOutputInfo(pSeUpdated, &curWin.winInfo);
    if (!rebuild) {
      winRows = updateCountWindowInfo(&curWin, tsCols, i, rows, pAggSup->windowCount, &rebuild);
    }

    if (rebuild) {
      doDeleteCountWindow(pAggSup, pSeUpdated, &curWin);
      int32_t rowSize = blockDataGetRowSize(pSDataBlock);
      for (int32_t j = i; j < rows; j++) {
        SResultRowData tmpRow = {0};
        tmpRow.pRowVal = taosMemoryCalloc(1, rowSize);
        transBlockToResultRow(pSDataBlock, j, 0, &tmpRow);
        int32_t len = 0;
        SRowDataKey key = {0};
        int32_t code = pAggSup->stateStore.streamStateCountDataPut(pAggSup->pDataState, &key, (const void*)tmpRow.pRowVal, &len);
      }
      *pCurWinInfo = curWin;
      break;
    }

    ASSERT(winRows >= 1);
    code = doOneWindowAggImpl(&pInfo->twAggSup.timeWindowData, &curWin.winInfo, &pResult, i, winRows, rows, numOfOutput,
                              pOperator, 0);
    if (code != TSDB_CODE_SUCCESS || pResult == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
    saveCountOutputBuf(pAggSup, &curWin.winInfo);

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE) {
      code = saveResult(curWin.winInfo, pSeUpdated);
    }

    if (pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE) {
      SSessionKey key = {0};
      getSessionHashKey(&curWin.winInfo.sessionWin, &key);
      tSimpleHashPut(pAggSup->pResultRows, &key, sizeof(SSessionKey), &curWin.winInfo, sizeof(SResultWindowInfo));
    }
  }
  return rebuild;
}

int32_t doStreamCountEncodeOpState(void** buf, int32_t len, SOperatorInfo* pOperator) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return 0;
  }

  void* pData = (buf == NULL) ? NULL : *buf;

  // 1.streamAggSup.pResultRows
  int32_t tlen = 0;
  int32_t mapSize = tSimpleHashGetSize(pInfo->streamAggSup.pResultRows);
  tlen += taosEncodeFixedI32(buf, mapSize);
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pInfo->streamAggSup.pResultRows, pIte, &iter)) != NULL) {
    void* key = tSimpleHashGetKey(pIte, &keyLen);
    tlen += encodeSSessionKey(buf, key);
    tlen += encodeSResultWindowInfo(buf, pIte, pInfo->streamAggSup.resultRowSize);
  }

  // 2.twAggSup
  tlen += encodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.dataVersion
  tlen += taosEncodeFixedI32(buf, pInfo->dataVersion);

  // 4.checksum
  if (buf) {
    uint32_t cksum = taosCalcChecksum(0, pData, len - sizeof(uint32_t));
    tlen += taosEncodeFixedU32(buf, cksum);
  } else {
    tlen += sizeof(uint32_t);
  }

  return tlen;
}

void* doStreamCountDecodeOpState(void* buf, int32_t len, SOperatorInfo* pOperator) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  if (!pInfo) {
    return buf;
  }

  // 4.checksum
  int32_t dataLen = len - sizeof(uint32_t);
  void*   pCksum = POINTER_SHIFT(buf, dataLen);
  if (taosCheckChecksum(buf, dataLen, *(uint32_t*)pCksum) != TSDB_CODE_SUCCESS) {
    ASSERT(0);  // debug
    qError("stream event state is invalid");
    return buf;
  }

  // 1.streamAggSup.pResultRows
  int32_t mapSize = 0;
  buf = taosDecodeFixedI32(buf, &mapSize);
  for (int32_t i = 0; i < mapSize; i++) {
    SSessionKey       key = {0};
    SResultWindowInfo winfo = {0};
    buf = decodeSSessionKey(buf, &key);
    buf = decodeSResultWindowInfo(buf, &winfo, pInfo->streamAggSup.resultRowSize);
    tSimpleHashPut(pInfo->streamAggSup.pResultRows, &key, sizeof(SSessionKey), &winfo, sizeof(SResultWindowInfo));
  }

  // 2.twAggSup
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);

  // 3.dataVersion
  buf = taosDecodeFixedI64(buf, &pInfo->dataVersion);
  return buf;
}

void doStreamCountSaveCheckpoint(SOperatorInfo* pOperator) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  int32_t                      len = doStreamCountEncodeOpState(NULL, 0, pOperator);
  void*                        buf = taosMemoryCalloc(1, len);
  void*                        pBuf = buf;
  len = doStreamCountEncodeOpState(&pBuf, len, pOperator);
  pInfo->streamAggSup.stateStore.streamStateSaveInfo(pInfo->streamAggSup.pWinState, STREAM_COUNT_OP_CHECKPOINT_NAME,
                                                     strlen(STREAM_COUNT_OP_CHECKPOINT_NAME), buf, len);
  taosMemoryFree(buf);
}

static SSDataBlock* buildCountResult(SOperatorInfo* pOperator) {
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;

  doBuildDeleteDataBlock(pOperator, pInfo->pSeDeleted, pInfo->pDelRes, &pInfo->pDelIterator);
  if (pInfo->pDelRes->info.rows > 0) {
    printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pInfo->pDelRes;
  }

  // todo(liuyao) 这里可能需要修改创建结果的代码
  doBuildSessionResult(pOperator, pInfo->streamAggSup.pWinState, &pInfo->groupResInfo, pBInfo->pRes);
  if (pBInfo->pRes->info.rows > 0) {
    printDataBlock(pBInfo->pRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pBInfo->pRes;
  }
  return NULL;
}

static SStreamStateCur* getDataCursorFromStreamStateBuff(SStreamCountAggSupporter* pAggSup,
                                                         SCountWindowInfo*         pWinInfo) {
  SRowDataKey key = {.groupId = pWinInfo->winInfo.sessionWin.groupId,
                     .ts = pWinInfo->winInfo.sessionWin.win.skey,
                     .uid = pWinInfo->startWinUid};
  return pAggSup->stateStore.streamStateCountDataSeekKeyCurrentNext(pAggSup->pDataState, &key);
}

static void getDataFromStreamStateBuff(SStreamCountAggSupporter* pAggSup, SStreamStateCur* pCur, SSDataBlock* pBlock) {
  while (pBlock->info.rows < pBlock->info.capacity) {
    SRowDataKey key = {0};
    void*       pVal = NULL;
    int32_t     len = 0;
    int32_t     code = pAggSup->stateStore.streamStateCountGetDataKVByCur(pAggSup->pDataState, &key, &pVal, &len);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    int32_t numOfCols = blockDataGetNumOfCols(pBlock);
    SResultRowData dataRow = {.key = key.ts, .pRowVal = pVal};
    for (int32_t slotId = 0; slotId < numOfCols; ++slotId) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
      SResultCellData* pCell = getResultCell(&dataRow, slotId);
      setRowCell(pColData, pBlock->info.rows, pCell);
    }
    pBlock->info.rows++;
  }
}

static void deleteCountWinState(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SSHashObj* pMapUpdate,
                           SSHashObj* pMapDelete) {
  SArray* pWins = taosArrayInit(16, sizeof(SCountWinKey));
  doDeleteTimeWindows(pAggSup, pBlock, pWins);
  removeSessionResults(pAggSup, pMapUpdate, pWins);
  copyDeleteWindowInfo(pWins, pMapDelete);
  taosArrayDestroy(pWins);
}

static SSDataBlock* doStreamCountAgg(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExprSupp*                   pSup = &pOperator->exprSupp;
  SStreamCountAggOperatorInfo* pInfo = pOperator->info;
  SOptrBasicInfo*              pBInfo = &pInfo->binfo;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  qDebug("===stream=== stream count agg");
  if (pOperator->status == OP_RES_TO_RETURN) {
    SSDataBlock* resBlock = buildCountResult(pOperator);
    if (resBlock != NULL) {
      return resBlock;
    }

    if (pInfo->reCkBlock) {
      pInfo->reCkBlock = false;
      printDataBlock(pInfo->pCheckpointRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
      return pInfo->pCheckpointRes;
    }

    if (pInfo->recvGetAll) {
      pInfo->recvGetAll = false;
      resetUnCloseSessionWinInfo(pInfo->streamAggSup.pResultRows);
    }

    setOperatorCompleted(pOperator);
    return NULL;
  }

  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (!pInfo->pUpdated) {
    pInfo->pUpdated = taosArrayInit(16, sizeof(SCountWindowInfo));
  }
  if (!pInfo->pSeUpdated) {
    _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    pInfo->pSeUpdated = tSimpleHashInit(64, hashFn);
  }
  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));

    if (pBlock->info.type == STREAM_DELETE_DATA || pBlock->info.type == STREAM_DELETE_RESULT ||
        pBlock->info.type == STREAM_CLEAR) {
      deleteSessionWinState(&pInfo->streamAggSup, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted);
      // todo(liuyao) 这里需要删除缓存的数据
      continue;
    } else if (pBlock->info.type == STREAM_GET_ALL) {
      pInfo->recvGetAll = true;
      getAllSessionWindow(pInfo->streamAggSup.pResultRows, pInfo->pSeUpdated);
      continue;
    } else if (pBlock->info.type == STREAM_CREATE_CHILD_TABLE) {
      return pBlock;
    } else if (pBlock->info.type == STREAM_CHECKPOINT) {
      pInfo->streamAggSup.stateStore.streamStateCommit(pInfo->streamAggSup.pWinState);
      pInfo->streamAggSup.stateStore.streamStateCommit(pInfo->streamAggSup.pDataState);
      doStreamCountSaveCheckpoint(pOperator);
      pInfo->reCkBlock = true;
      copyDataBlock(pInfo->pCheckpointRes, pBlock);
      continue;
    } else {
      ASSERTS(pBlock->info.type == STREAM_NORMAL || pBlock->info.type == STREAM_INVALID, "invalid SSDataBlock type");
    }

    if (pInfo->scalarSupp.pExprInfo != NULL) {
      SExprSupp* pExprSup = &pInfo->scalarSupp;
      projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    }
    setInputDataBlock(pSup, pBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
    SCountWindowInfo rebuildWin = {0};
    bool rebuild = doStreamCountAggImpl(pOperator, pBlock, pInfo->pSeUpdated, pInfo->pSeDeleted, &rebuildWin);
    if (rebuild) {
      SStreamStateCur* pCur = getDataCursorFromStreamStateBuff(&pInfo->streamAggSup, &rebuildWin);
      SSDataBlock* pTempBlock = createOneDataBlock(pBlock, false);
      while (1) {
        getDataFromStreamStateBuff(&pInfo->streamAggSup, pCur, pTempBlock);
        if (pTempBlock->info.rows == 0) {
          pInfo->streamAggSup.stateStore.streamStateFreeCur(pCur);
          break;
        }
        setInputDataBlock(&pOperator->exprSupp, pTempBlock, TSDB_ORDER_ASC, MAIN_SCAN, true);
        rebuildWin = (SCountWindowInfo){0};
        doStreamCountAggImpl(pOperator, pTempBlock, pInfo->pSeUpdated, pInfo->pSeDeleted, &rebuildWin);
      }
    }
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlock->info.window.ekey);
  }
  // restore the value
  pOperator->status = OP_RES_TO_RETURN;

  closeSessionWindow(pInfo->streamAggSup.pResultRows, &pInfo->twAggSup, pInfo->pSeUpdated);
  copyUpdateResult(&pInfo->pSeUpdated, pInfo->pUpdated, sessionKeyCompareAsc);
  removeSessionDeleteResults(pInfo->pSeDeleted, pInfo->pUpdated);

  initGroupResInfoFromArrayList(&pInfo->groupResInfo, pInfo->pUpdated);
  pInfo->pUpdated = NULL;
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  SSDataBlock* resBlock = buildCountResult(pOperator);
  if (resBlock != NULL) {
    return resBlock;
  }
  setOperatorCompleted(pOperator);
  return NULL;
}

void streamCountReleaseState(SOperatorInfo* pOperator) {
  // do nothing
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.releaseStreamStateFn) {
    downstream->fpSet.releaseStreamStateFn(downstream);
  }
}

void streamCountReloadState(SOperatorInfo* pOperator) {
  // do nothing
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  if (downstream->fpSet.reloadStreamStateFn) {
    downstream->fpSet.reloadStreamStateFn(downstream);
  }
}

static TSKEY countDataTs(void* pKey) {
  SRowDataKey* pWinKey = (SRowDataKey*)pKey;
  return pWinKey->ts;
}

int32_t initStreamCountAggSupporter(SStreamCountAggSupporter* pSup, SExprSupp* pExpSup, int32_t numOfOutput, int64_t gap,
                                    SStreamState* pState, int32_t keySize, int16_t keyType, SStateStore* pStore,
                                    SReadHandle* pHandle, STimeWindowAggSupp* pTwAggSup, const char* taskIdStr,
                                    SStorageAPI* pApi) {
  pSup->resultRowSize = keySize + getResultRowSize(pExpSup->pCtx, numOfOutput);
  pSup->pScanBlock = createSpecialDataBlock(STREAM_CLEAR);

  pSup->stateStore = *pStore;

  pSup->pDataState = taosMemoryCalloc(1, sizeof(SStreamState));
  *(pSup->pDataState) = *pState;
  pSup->stateStore.streamStateSetNumber(pSup->pDataState, -1);
  int32_t funResSize = getMaxFunResSize(pExpSup, numOfOutput);
  // todo(liuyao) 这里需要初始化缓存，包括selectivity函数的缓存。
  pSup->pDataState->pFileState = pSup->stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SRowDataKey), pSup->resultRowSize, funResSize, countDataTs, pSup->pDataState,
      pTwAggSup->deleteMark, taskIdStr, pHandle->checkpointId, STREAM_STATE_BUFF_SORT);
  
  pSup->pWinState = taosMemoryCalloc(1, sizeof(SStreamState));
  *(pSup->pWinState) = *pState;
  pSup->pWinState->pFileState = pSup->stateStore.streamFileStateInit(
      tsStreamBufferSize, sizeof(SCountWinKey), pSup->resultRowSize, funResSize, countDataTs, pSup->pWinState,
      pTwAggSup->deleteMark, taskIdStr, pHandle->checkpointId, STREAM_STATE_BUFF_SORT);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pSup->pResultRows = tSimpleHashInit(32, hashFn);

  pSup->pSessionAPI = pApi;

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createStreamCountAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode,
                                                SExecTaskInfo* pTaskInfo, SReadHandle* pHandle) {
  SStreamCountWinodwPhysiNode* pCountNode = (SStreamCountWinodwPhysiNode*)pPhyNode;
  int32_t                      tsSlotId = ((SColumnNode*)pCountNode->window.pTspk)->slotId;
  int32_t                      code = TSDB_CODE_SUCCESS;
  SStreamCountAggOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamCountAggOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  if (pCountNode->window.pExprs != NULL) {
    int32_t    numOfScalar = 0;
    SExprInfo* pScalarExprInfo = createExprInfo(pCountNode->window.pExprs, NULL, &numOfScalar);
    code = initExprSupp(&pInfo->scalarSupp, pScalarExprInfo, numOfScalar, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pCountNode->window.watermark,
      .calTrigger = pCountNode->window.triggerType,
      .maxTs = INT64_MIN,
      .minTs = INT64_MAX,
      .deleteMark = getDeleteMark(&pCountNode->window, 0),
  };

  initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);

  SExprSupp*   pExpSup = &pOperator->exprSupp;
  int32_t      numOfCols = 0;
  SExprInfo*   pExprInfo = createExprInfo(pCountNode->window.pFuncs, NULL, &numOfCols);
  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  code = initBasicInfoEx(&pInfo->binfo, pExpSup, pExprInfo, numOfCols, pResBlock, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initStreamAggSupporter(&pInfo->streamAggSup, pExpSup, numOfCols, 0, pTaskInfo->streamInfo.pState,
                                sizeof(bool) + sizeof(bool), 0, &pTaskInfo->storageAPI.stateStore, pHandle,
                                &pInfo->twAggSup, GET_TASKID(pTaskInfo), &pTaskInfo->storageAPI);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->primaryTsIndex = tsSlotId;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pSeDeleted = tSimpleHashInit(64, hashFn);
  pInfo->pDelIterator = NULL;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  pInfo->ignoreExpiredData = pCountNode->window.igExpired;
  pInfo->ignoreExpiredDataSaved = false;
  pInfo->pUpdated = NULL;
  pInfo->pSeUpdated = NULL;
  pInfo->dataVersion = 0;
  pInfo->historyWins = taosArrayInit(4, sizeof(SSessionKey));
  if (!pInfo->historyWins) {
    goto _error;
  }

  pInfo->pCheckpointRes = createSpecialDataBlock(STREAM_CHECKPOINT);
  pInfo->reCkBlock = false;
  pInfo->recvGetAll = false;

  // for stream
  void*   buff = NULL;
  int32_t len = 0;
  int32_t res =
      pInfo->streamAggSup.stateStore.streamStateGetInfo(pInfo->streamAggSup.pState, STREAM_COUNT_OP_CHECKPOINT_NAME,
                                                        strlen(STREAM_COUNT_OP_CHECKPOINT_NAME), &buff, &len);
  if (res == TSDB_CODE_SUCCESS) {
    doStreamCountDecodeOpState(buff, len, pOperator);
    taosMemoryFree(buff);
  }

  setOperatorInfo(pOperator, "StreamCountAggOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT, true, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamCountAgg, NULL, destroyStreamCountOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamCountReleaseState, streamCountReloadState);
  initDownStream(downstream, &pInfo->streamAggSup, pOperator->operatorType, pInfo->primaryTsIndex, &pInfo->twAggSup);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  destroyStreamCountOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}
