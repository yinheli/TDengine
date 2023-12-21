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
#define _DEFAULT_SOURCE
#include "tarray.h"
#include "monInt.h"
#include "taoserror.h"
#include "thttp.h"
#include "ttime.h"
#include "taos_monitor.h"
#include "tglobal.h"

SMonitor tsMonitor = {0};
static char* tsMonUri = "/report";
static char* tsMonFwUri = "/td_metric";

#define MASTER_UPTIME  "cluster_info:master_uptime"
#define DBS_TOTAL "cluster_info:dbs_total"
#define TBS_TOTAL "cluster_info:tbs_total"
#define STBS_TOTAL "cluster_info:stbs_total"
#define VGROUPS_TOTAL "cluster_info:vgroups_total"
#define VGROUPS_ALIVE "cluster_info:vgroups_alive"
#define VNODES_TOTAL "cluster_info:vnodes_total"
#define VNODES_ALIVE "cluster_info:vnodes_alive"
#define DNODES_TOTAL "cluster_info:dnodes_total"
#define DNODES_ALIVE "cluster_info:dnodes_alive"
#define CONNECTIONS_TOTAL "cluster_info:connections_total"
#define TOPICS_TOTAL "cluster_info:topics_total"
#define STREAMS_TOTAL "cluster_info:streams_total"

#define TABLES_NUM "cluster_vgroups_info:tables_num"
#define STATUS "cluster_vgroups_info:status"

#define UPTIME "dnodes_info:uptime"
#define CPU_ENGINE "dnodes_info:cpu_engine"
#define CPU_SYSTEM "dnodes_info:cpu_system"
#define CPU_CORES "dnodes_info:cpu_cores"
#define MEM_ENGINE "dnodes_info:mem_engine"
#define MEM_SYSTEM "dnodes_info:mem_system"
#define MEM_TOTAL "dnodes_info:mem_total"
#define DISK_ENGINE "dnodes_info:disk_engine"
#define DISK_USED "dnodes_info:disk_used"
#define DISK_TOTAL "dnodes_info:disk_total"
#define NET_IN "dnodes_info:net_in"
#define NET_OUT "dnodes_info:net_out"
#define IO_READ "dnodes_info:io_read"
#define IO_WRITE "dnodes_info:io_write"
#define IO_READ_DISK "dnodes_info:io_read_disk"
#define IO_WRITE_DISK "dnodes_info:io_write_disk"
#define REQ_SELECT "req_select"
#define REQ_SELECT_RATE "req_select_rate"
#define REQ_INSERT "req_insert"
#define REQ_INSERT_SUCCESS "req_insert_success"
#define REQ_INSERT_RATE "req_insert_rate"
#define REQ_INSERT_BATCH "req_insert_batch"
#define REQ_INSERT_BATCH_SUCCESS "req_insert_batch_success"
#define REQ_INSERT_BATCH_RATE "req_insert_batch_rate"
#define ERRORS "dnodes_info:errors"
#define VNODES_NUM "dnodes_info:vnodes_num"
#define MASTERS "dnodes_info:masters"
#define HAS_MNODE "dnodes_info:has_mnode"
#define HAS_QNODE "dnodes_info:has_qnode"
#define HAS_SNODE "dnodes_info:has_snode"
#define DNODE_STATUS "dnodes_info:status"

void monRecordLog(int64_t ts, ELogLevel level, const char *content) {
  taosThreadMutexLock(&tsMonitor.lock);
  int32_t size = taosArrayGetSize(tsMonitor.logs);
  if (size < tsMonitor.cfg.maxLogs) {
    SMonLogItem  item = {.ts = ts, .level = level};
    SMonLogItem *pItem = taosArrayPush(tsMonitor.logs, &item);
    if (pItem != NULL) {
      tstrncpy(pItem->content, content, MON_LOG_LEN);
    }
  }
  taosThreadMutexUnlock(&tsMonitor.lock);
}

int32_t monGetLogs(SMonLogs *logs) {
  taosThreadMutexLock(&tsMonitor.lock);
  logs->logs = taosArrayDup(tsMonitor.logs, NULL);
  logs->numOfInfoLogs = tsNumOfInfoLogs;
  logs->numOfErrorLogs = tsNumOfErrorLogs;
  logs->numOfDebugLogs = tsNumOfDebugLogs;
  logs->numOfTraceLogs = tsNumOfTraceLogs;
  tsNumOfInfoLogs = 0;
  tsNumOfErrorLogs = 0;
  tsNumOfDebugLogs = 0;
  tsNumOfTraceLogs = 0;
  taosArrayClear(tsMonitor.logs);
  taosThreadMutexUnlock(&tsMonitor.lock);
  if (logs->logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return 0;
}

void monSetDmInfo(SMonDmInfo *pInfo) {
  taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.dmInfo, pInfo, sizeof(SMonDmInfo));
  taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonDmInfo));
}

void monSetMmInfo(SMonMmInfo *pInfo) {
  taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.mmInfo, pInfo, sizeof(SMonMmInfo));
  taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonMmInfo));
}

void monSetVmInfo(SMonVmInfo *pInfo) {
  taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.vmInfo, pInfo, sizeof(SMonVmInfo));
  taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonVmInfo));
}

void monSetQmInfo(SMonQmInfo *pInfo) {
  taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.qmInfo, pInfo, sizeof(SMonQmInfo));
  taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonQmInfo));
}

void monSetSmInfo(SMonSmInfo *pInfo) {
  taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.smInfo, pInfo, sizeof(SMonSmInfo));
  taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonSmInfo));
}

void monSetBmInfo(SMonBmInfo *pInfo) {
  taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.bmInfo, pInfo, sizeof(SMonBmInfo));
  taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonBmInfo));
}

int32_t monInit(const SMonCfg *pCfg) {
  tsMonitor.logs = taosArrayInit(16, sizeof(SMonLogItem));
  if (tsMonitor.logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tsMonitor.cfg = *pCfg;
  tsLogFp = monRecordLog;
  tsMonitor.lastTime = taosGetTimestampMs();
  taosThreadMutexInit(&tsMonitor.lock, NULL);

  taos_collector_registry_default_init();

  monInitNewMonitor();

  return 0;
}

void monCleanup() {
  tsLogFp = NULL;
  taosArrayDestroy(tsMonitor.logs);
  tsMonitor.logs = NULL;
  tFreeSMonMmInfo(&tsMonitor.mmInfo);
  tFreeSMonVmInfo(&tsMonitor.vmInfo);
  tFreeSMonSmInfo(&tsMonitor.smInfo);
  tFreeSMonQmInfo(&tsMonitor.qmInfo);
  tFreeSMonBmInfo(&tsMonitor.bmInfo);
  taosThreadMutexDestroy(&tsMonitor.lock);

  taosHashCleanup(tsMonitor.metrics);
  taos_collector_registry_destroy(TAOS_COLLECTOR_REGISTRY_DEFAULT);
  TAOS_COLLECTOR_REGISTRY_DEFAULT = NULL;
}

static void monCleanupMonitorInfo(SMonInfo *pMonitor) {
  tsMonitor.lastTime = pMonitor->curTime;
  taosArrayDestroy(pMonitor->log.logs);
  tFreeSMonMmInfo(&pMonitor->mmInfo);
  tFreeSMonVmInfo(&pMonitor->vmInfo);
  tFreeSMonSmInfo(&pMonitor->smInfo);
  tFreeSMonQmInfo(&pMonitor->qmInfo);
  tFreeSMonBmInfo(&pMonitor->bmInfo);
  tjsonDelete(pMonitor->pJson);
  taosMemoryFree(pMonitor);
}

static SMonInfo *monCreateMonitorInfo() {
  SMonInfo *pMonitor = taosMemoryCalloc(1, sizeof(SMonInfo));
  if (pMonitor == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  monGetLogs(&pMonitor->log);

  taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&pMonitor->dmInfo, &tsMonitor.dmInfo, sizeof(SMonDmInfo));
  memcpy(&pMonitor->mmInfo, &tsMonitor.mmInfo, sizeof(SMonMmInfo));
  memcpy(&pMonitor->vmInfo, &tsMonitor.vmInfo, sizeof(SMonVmInfo));
  memcpy(&pMonitor->smInfo, &tsMonitor.smInfo, sizeof(SMonSmInfo));
  memcpy(&pMonitor->qmInfo, &tsMonitor.qmInfo, sizeof(SMonQmInfo));
  memcpy(&pMonitor->bmInfo, &tsMonitor.bmInfo, sizeof(SMonBmInfo));
  memset(&tsMonitor.dmInfo, 0, sizeof(SMonDmInfo));
  memset(&tsMonitor.mmInfo, 0, sizeof(SMonMmInfo));
  memset(&tsMonitor.vmInfo, 0, sizeof(SMonVmInfo));
  memset(&tsMonitor.smInfo, 0, sizeof(SMonSmInfo));
  memset(&tsMonitor.qmInfo, 0, sizeof(SMonQmInfo));
  memset(&tsMonitor.bmInfo, 0, sizeof(SMonBmInfo));
  taosThreadMutexUnlock(&tsMonitor.lock);

  pMonitor->pJson = tjsonCreateObject();
  if (pMonitor->pJson == NULL || pMonitor->log.logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    monCleanupMonitorInfo(pMonitor);
    return NULL;
  }

  pMonitor->curTime = taosGetTimestampMs();
  pMonitor->lastTime = tsMonitor.lastTime;
  return pMonitor;
}

static void monGenBasicJson(SMonInfo *pMonitor) {
  SMonBasicInfo *pInfo = &pMonitor->dmInfo.basic;

  SJson *pJson = pMonitor->pJson;
  char   buf[40] = {0};
  taosFormatUtcTime(buf, sizeof(buf), pMonitor->curTime, TSDB_TIME_PRECISION_MILLI);

  tjsonAddStringToObject(pJson, "ts", buf);
  tjsonAddDoubleToObject(pJson, "dnode_id", pInfo->dnode_id);
  tjsonAddStringToObject(pJson, "dnode_ep", pInfo->dnode_ep);
  snprintf(buf, sizeof(buf), "%" PRId64, pInfo->cluster_id);
  tjsonAddStringToObject(pJson, "cluster_id", buf);
  tjsonAddDoubleToObject(pJson, "protocol", pInfo->protocol);
}

static void monGenClusterJson(SMonInfo *pMonitor) {
  SMonClusterInfo *pInfo = &pMonitor->mmInfo.cluster;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "cluster_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  tjsonAddStringToObject(pJson, "first_ep", pInfo->first_ep);
  tjsonAddDoubleToObject(pJson, "first_ep_dnode_id", pInfo->first_ep_dnode_id);
  tjsonAddStringToObject(pJson, "version", pInfo->version);
  tjsonAddDoubleToObject(pJson, "master_uptime", pInfo->master_uptime);
  tjsonAddDoubleToObject(pJson, "monitor_interval", pInfo->monitor_interval);
  tjsonAddDoubleToObject(pJson, "dbs_total", pInfo->dbs_total);
  tjsonAddDoubleToObject(pJson, "tbs_total", pInfo->tbs_total);
  tjsonAddDoubleToObject(pJson, "stbs_total", pInfo->stbs_total);
  tjsonAddDoubleToObject(pJson, "vgroups_total", pInfo->vgroups_total);
  tjsonAddDoubleToObject(pJson, "vgroups_alive", pInfo->vgroups_alive);
  tjsonAddDoubleToObject(pJson, "vnodes_total", pInfo->vnodes_total);
  tjsonAddDoubleToObject(pJson, "vnodes_alive", pInfo->vnodes_alive);
  tjsonAddDoubleToObject(pJson, "connections_total", pInfo->connections_total);
  tjsonAddDoubleToObject(pJson, "topics_total", pInfo->topics_toal);
  tjsonAddDoubleToObject(pJson, "streams_total", pInfo->streams_total);

  monGenClusterInfoTable(&pMonitor->dmInfo.basic, pInfo);

  /*
  int32_t monSize = taosArrayGetSize(pInfo->clientMetrics);
  for(int32_t i = 0; i < monSize; i++){
    taos_counter_t* metric = taosArrayGet(pInfo->clientMetrics, i);

    char* arr[2] = {0};
    taos_monitor_split_str_metric((char**)&arr, metric, ":");

    if(strcmp(arr[0], "cluster_info") != 0) continue;

    taos_metric_formatter_load_metric_custom(metric, pJson);
  }
  */

  void *pIter = taosHashIterate(pInfo->monitor_client_metrics, NULL);
  while (pIter) {
    taos_counter_t** metric = (taos_counter_t**)pIter;

    taos_metric_formatter_load_metric_custom(*metric, pJson);

    pIter = taosHashIterate(pInfo->monitor_client_metrics, pIter);
  }

  SJson *pDnodesJson = tjsonAddArrayToObject(pJson, "dnodes");
  if (pDnodesJson == NULL) return;

  char cluster_id[TSDB_CLUSTER_ID_LEN] = {0};
  snprintf(cluster_id, sizeof(cluster_id), "%" PRId64, pMonitor->dmInfo.basic.cluster_id);
  taos_gauge_t **metric = NULL;

  int32_t dnode_total = taosArrayGetSize(pInfo->dnodes);
  int32_t dnode_alive = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->dnodes); ++i) {
    SJson *pDnodeJson = tjsonCreateObject();
    if (pDnodeJson == NULL) continue;

    SMonDnodeDesc *pDnodeDesc = taosArrayGet(pInfo->dnodes, i);
    tjsonAddDoubleToObject(pDnodeJson, "dnode_id", pDnodeDesc->dnode_id);
    tjsonAddStringToObject(pDnodeJson, "dnode_ep", pDnodeDesc->dnode_ep);
    tjsonAddStringToObject(pDnodeJson, "status", pDnodeDesc->status);

    if (tjsonAddItemToArray(pDnodesJson, pDnodeJson) != 0) tjsonDelete(pDnodeJson);

    if(pMonitor->dmInfo.basic.cluster_id != 0){
      char dnode_id[50] = {0};
      snprintf(dnode_id, sizeof(dnode_id), "%d", pDnodeDesc->dnode_id);

      const char *sample_labels[] = {cluster_id, dnode_id, pDnodeDesc->dnode_ep};

      metric = taosHashGet(tsMonitor.metrics, DNODE_STATUS, strlen(DNODE_STATUS));

      int32_t status = 0;
      if(strcmp(pDnodeDesc->status, "ready") == 0){
        status = 1;
        dnode_alive++;
      }
      taos_gauge_set(*metric, status, sample_labels);
    } 
  }

  SJson *pMnodesJson = tjsonAddArrayToObject(pJson, "mnodes");
  if (pMnodesJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->mnodes); ++i) {
    SJson *pMnodeJson = tjsonCreateObject();
    if (pMnodeJson == NULL) continue;

    SMonMnodeDesc *pMnodeDesc = taosArrayGet(pInfo->mnodes, i);
    tjsonAddDoubleToObject(pMnodeJson, "mnode_id", pMnodeDesc->mnode_id);
    tjsonAddStringToObject(pMnodeJson, "mnode_ep", pMnodeDesc->mnode_ep);
    tjsonAddStringToObject(pMnodeJson, "role", pMnodeDesc->role);

    if (tjsonAddItemToArray(pMnodesJson, pMnodeJson) != 0) tjsonDelete(pMnodeJson);
  }
}

static void monGenVgroupInfoTable(SMonInfo *pMonitor){
  if(pMonitor->dmInfo.basic.cluster_id == 0) return;

  SMonVgroupInfo *pInfo = &pMonitor->mmInfo.vgroup;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  char cluster_id[TSDB_CLUSTER_ID_LEN];
  snprintf(cluster_id, sizeof(cluster_id), "%" PRId64, pMonitor->dmInfo.basic.cluster_id);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vgroups); ++i) {
    SMonVgroupDesc *pVgroupDesc = taosArrayGet(pInfo->vgroups, i);

    char vgroup_id[50];
    snprintf(vgroup_id, sizeof(vgroup_id), "%" PRId64, pMonitor->dmInfo.basic.cluster_id);

    const char *sample_labels[] = {cluster_id, vgroup_id, pVgroupDesc->database_name};

    taos_gauge_t **metric = NULL;
  
    metric = taosHashGet(tsMonitor.metrics, TABLES_NUM, strlen(TABLES_NUM));
    taos_gauge_set(*metric, pVgroupDesc->tables_num, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, STATUS, strlen(STATUS));
    int32_t status = 0;
    if(strcmp(pVgroupDesc->status, "ready") == 0){
      status = 1;
    }
    taos_gauge_set(*metric, status, sample_labels);
 }
}

static void monGenVgroupJson(SMonInfo *pMonitor) {
  SMonVgroupInfo *pInfo = &pMonitor->mmInfo.vgroup;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonAddArrayToObject(pMonitor->pJson, "vgroup_infos");
  if (pJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vgroups); ++i) {
    SJson *pVgroupJson = tjsonCreateObject();
    if (pVgroupJson == NULL) continue;
    if (tjsonAddItemToArray(pJson, pVgroupJson) != 0) {
      tjsonDelete(pVgroupJson);
      continue;
    }

    SMonVgroupDesc *pVgroupDesc = taosArrayGet(pInfo->vgroups, i);
    tjsonAddDoubleToObject(pVgroupJson, "vgroup_id", pVgroupDesc->vgroup_id);
    tjsonAddStringToObject(pVgroupJson, "database_name", pVgroupDesc->database_name);
    tjsonAddDoubleToObject(pVgroupJson, "tables_num", pVgroupDesc->tables_num);
    tjsonAddStringToObject(pVgroupJson, "status", pVgroupDesc->status);

    SJson *pVnodesJson = tjsonAddArrayToObject(pVgroupJson, "vnodes");
    if (pVnodesJson == NULL) continue;

    for (int32_t j = 0; j < TSDB_MAX_REPLICA; ++j) {
      SMonVnodeDesc *pVnodeDesc = &pVgroupDesc->vnodes[j];
      if (pVnodeDesc->dnode_id <= 0) continue;

      SJson *pVnodeJson = tjsonCreateObject();
      if (pVnodeJson == NULL) continue;

      tjsonAddDoubleToObject(pVnodeJson, "dnode_id", pVnodeDesc->dnode_id);
      tjsonAddStringToObject(pVnodeJson, "vnode_role", pVnodeDesc->vnode_role);

      if (tjsonAddItemToArray(pVnodesJson, pVnodeJson) != 0) tjsonDelete(pVnodeJson);
    }
  }

  monGenVgroupInfoTable(pMonitor);
}

static void monGenStbJson(SMonInfo *pMonitor) {
  SMonStbInfo *pInfo = &pMonitor->mmInfo.stb;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonAddArrayToObject(pMonitor->pJson, "stb_infos");
  if (pJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->stbs); ++i) {
    SJson *pStbJson = tjsonCreateObject();
    if (pStbJson == NULL) continue;
    if (tjsonAddItemToArray(pJson, pStbJson) != 0) {
      tjsonDelete(pStbJson);
      continue;
    }

    SMonStbDesc *pStbDesc = taosArrayGet(pInfo->stbs, i);
    tjsonAddStringToObject(pStbJson, "stb_name", pStbDesc->stb_name);
    tjsonAddStringToObject(pStbJson, "database_name", pStbDesc->database_name);
  }
}

static void monGenGrantJson(SMonInfo *pMonitor) {
  SMonGrantInfo *pInfo = &pMonitor->mmInfo.grant;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "grant_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  tjsonAddDoubleToObject(pJson, "expire_time", pInfo->expire_time);
  tjsonAddDoubleToObject(pJson, "timeseries_used", pInfo->timeseries_used);
  tjsonAddDoubleToObject(pJson, "timeseries_total", pInfo->timeseries_total);
}

static void monGenDnodeJson(SMonInfo *pMonitor) {
  SMonDnodeInfo *pInfo = &pMonitor->dmInfo.dnode;
  SMonSysInfo   *pSys = &pMonitor->dmInfo.sys;
  SVnodesStat   *pStat = &pMonitor->vmInfo.vstat;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "dnode_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  double interval = (pMonitor->curTime - pMonitor->lastTime) / 1000.0;
  if (pMonitor->curTime - pMonitor->lastTime == 0) {
    interval = 1;
  }

  double cpu_engine = 0;
  double mem_engine = 0;
  double net_in = 0;
  double net_out = 0;
  double io_read = 0;
  double io_write = 0;
  double io_read_disk = 0;
  double io_write_disk = 0;

  SMonSysInfo *sysArrays[6];
  sysArrays[0] = &pMonitor->dmInfo.sys;
  sysArrays[1] = &pMonitor->mmInfo.sys;
  sysArrays[2] = &pMonitor->vmInfo.sys;
  sysArrays[3] = &pMonitor->qmInfo.sys;
  sysArrays[4] = &pMonitor->smInfo.sys;
  sysArrays[5] = &pMonitor->bmInfo.sys;
  for (int32_t i = 0; i < 6; ++i) {
    cpu_engine += sysArrays[i]->cpu_engine;
    mem_engine += sysArrays[i]->mem_engine;
    net_in += sysArrays[i]->net_in;
    net_out += sysArrays[i]->net_out;
    io_read += sysArrays[i]->io_read;
    io_write += sysArrays[i]->io_write;
    io_read_disk += sysArrays[i]->io_read_disk;
    io_write_disk += sysArrays[i]->io_write_disk;
  }

  double req_select_rate = pStat->numOfSelectReqs / interval;
  double req_insert_rate = pStat->numOfInsertReqs / interval;
  double req_insert_batch_rate = pStat->numOfBatchInsertReqs / interval;
  double net_in_rate = net_in / interval;
  double net_out_rate = net_out / interval;
  double io_read_rate = io_read / interval;
  double io_write_rate = io_write / interval;
  double io_read_disk_rate = io_read_disk / interval;
  double io_write_disk_rate = io_write_disk / interval;

  tjsonAddDoubleToObject(pJson, "uptime", pInfo->uptime);
  tjsonAddDoubleToObject(pJson, "cpu_engine", cpu_engine);
  tjsonAddDoubleToObject(pJson, "cpu_system", pSys->cpu_system);
  tjsonAddDoubleToObject(pJson, "cpu_cores", pSys->cpu_cores);
  tjsonAddDoubleToObject(pJson, "mem_engine", mem_engine);
  tjsonAddDoubleToObject(pJson, "mem_system", pSys->mem_system);
  tjsonAddDoubleToObject(pJson, "mem_total", pSys->mem_total);
  tjsonAddDoubleToObject(pJson, "disk_engine", pSys->disk_engine);
  tjsonAddDoubleToObject(pJson, "disk_used", pSys->disk_used);
  tjsonAddDoubleToObject(pJson, "disk_total", pSys->disk_total);
  tjsonAddDoubleToObject(pJson, "net_in", net_in_rate);
  tjsonAddDoubleToObject(pJson, "net_out", net_out_rate);
  tjsonAddDoubleToObject(pJson, "io_read", io_read_rate);
  tjsonAddDoubleToObject(pJson, "io_write", io_write_rate);
  tjsonAddDoubleToObject(pJson, "io_read_disk", io_read_disk_rate);
  tjsonAddDoubleToObject(pJson, "io_write_disk", io_write_disk_rate);
  tjsonAddDoubleToObject(pJson, "req_select", pStat->numOfSelectReqs);
  tjsonAddDoubleToObject(pJson, "req_select_rate", req_select_rate);
  tjsonAddDoubleToObject(pJson, "req_insert", pStat->numOfInsertReqs);
  tjsonAddDoubleToObject(pJson, "req_insert_success", pStat->numOfInsertSuccessReqs);
  tjsonAddDoubleToObject(pJson, "req_insert_rate", req_insert_rate);
  tjsonAddDoubleToObject(pJson, "req_insert_batch", pStat->numOfBatchInsertReqs);
  tjsonAddDoubleToObject(pJson, "req_insert_batch_success", pStat->numOfBatchInsertSuccessReqs);
  tjsonAddDoubleToObject(pJson, "req_insert_batch_rate", req_insert_batch_rate);
  tjsonAddDoubleToObject(pJson, "errors", pStat->errors);
  tjsonAddDoubleToObject(pJson, "vnodes_num", pStat->totalVnodes);
  tjsonAddDoubleToObject(pJson, "masters", pStat->masterNum);
  tjsonAddDoubleToObject(pJson, "has_mnode", pInfo->has_mnode);
  tjsonAddDoubleToObject(pJson, "has_qnode", pInfo->has_qnode);
  tjsonAddDoubleToObject(pJson, "has_snode", pInfo->has_snode);

  if(pMonitor->dmInfo.basic.cluster_id != 0){
    char cluster_id[TSDB_CLUSTER_ID_LEN];
    snprintf(cluster_id, sizeof(cluster_id), "%" PRId64, pMonitor->dmInfo.basic.cluster_id);

    char dnode_id[50];
    snprintf(dnode_id, sizeof(dnode_id), "%d", pMonitor->dmInfo.basic.dnode_id);

    const char *sample_labels[] = {cluster_id, dnode_id, pMonitor->dmInfo.basic.dnode_ep};

    taos_gauge_t **metric = NULL;

    metric = taosHashGet(tsMonitor.metrics, UPTIME, strlen(UPTIME));
    taos_gauge_set(*metric, pInfo->uptime, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, CPU_ENGINE, strlen(CPU_ENGINE));
    taos_gauge_set(*metric, cpu_engine, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, CPU_SYSTEM, strlen(CPU_SYSTEM));
    taos_gauge_set(*metric, pSys->cpu_system, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, MEM_ENGINE, strlen(MEM_ENGINE));
    taos_gauge_set(*metric, mem_engine, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, MEM_SYSTEM, strlen(MEM_SYSTEM));
    taos_gauge_set(*metric, pSys->mem_system, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, DISK_ENGINE, strlen(DISK_ENGINE));
    taos_gauge_set(*metric, pSys->disk_engine, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, DISK_USED, strlen(DISK_USED));
    taos_gauge_set(*metric, pSys->disk_used, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, NET_IN, strlen(NET_IN));
    taos_gauge_set(*metric, net_in_rate, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, NET_OUT, strlen(NET_OUT));
    taos_gauge_set(*metric, net_out_rate, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, IO_READ, strlen(IO_READ));
    taos_gauge_set(*metric, io_read_rate, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, IO_WRITE, strlen(IO_WRITE));
    taos_gauge_set(*metric, io_write_rate, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, IO_READ_DISK, strlen(IO_READ_DISK));
    taos_gauge_set(*metric, io_read_disk_rate, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, IO_WRITE_DISK, strlen(IO_WRITE_DISK));
    taos_gauge_set(*metric, io_write_disk_rate, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, ERRORS, strlen(ERRORS));
    taos_gauge_set(*metric, io_read_disk_rate, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, VNODES_NUM, strlen(VNODES_NUM));
    taos_gauge_set(*metric, pStat->errors, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, MASTERS, strlen(MASTERS));
    taos_gauge_set(*metric, pStat->masterNum, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, HAS_MNODE, strlen(HAS_MNODE));
    taos_gauge_set(*metric, pInfo->has_mnode, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, HAS_QNODE, strlen(HAS_QNODE));
    taos_gauge_set(*metric, pInfo->has_qnode, sample_labels);

    metric = taosHashGet(tsMonitor.metrics, HAS_SNODE, strlen(HAS_SNODE));
    taos_gauge_set(*metric, pInfo->has_snode, sample_labels);
  }
}

static void monGenDiskJson(SMonInfo *pMonitor) {
  SMonDiskInfo *pInfo = &pMonitor->vmInfo.tfs;
  SMonDiskDesc *pLogDesc = &pMonitor->dmInfo.dnode.logdir;
  SMonDiskDesc *pTempDesc = &pMonitor->dmInfo.dnode.tempdir;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "disk_infos", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  SJson *pDatadirsJson = tjsonAddArrayToObject(pJson, "datadir");
  if (pDatadirsJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->datadirs); ++i) {
    SJson *pDatadirJson = tjsonCreateObject();
    if (pDatadirJson == NULL) continue;

    SMonDiskDesc *pDatadirDesc = taosArrayGet(pInfo->datadirs, i);
    if (tjsonAddStringToObject(pDatadirJson, "name", pDatadirDesc->name) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "level", pDatadirDesc->level) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "avail", pDatadirDesc->size.avail) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "used", pDatadirDesc->size.used) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "total", pDatadirDesc->size.total) != 0) tjsonDelete(pDatadirJson);

    if (tjsonAddItemToArray(pDatadirsJson, pDatadirJson) != 0) tjsonDelete(pDatadirJson);
  }

  SJson *pLogdirJson = tjsonCreateObject();
  if (pLogdirJson == NULL) return;
  if (tjsonAddItemToObject(pJson, "logdir", pLogdirJson) != 0) return;
  tjsonAddStringToObject(pLogdirJson, "name", pLogDesc->name);
  tjsonAddDoubleToObject(pLogdirJson, "avail", pLogDesc->size.avail);
  tjsonAddDoubleToObject(pLogdirJson, "used", pLogDesc->size.used);
  tjsonAddDoubleToObject(pLogdirJson, "total", pLogDesc->size.total);

  SJson *pTempdirJson = tjsonCreateObject();
  if (pTempdirJson == NULL) return;
  if (tjsonAddItemToObject(pJson, "tempdir", pTempdirJson) != 0) return;
  tjsonAddStringToObject(pTempdirJson, "name", pTempDesc->name);
  tjsonAddDoubleToObject(pTempdirJson, "avail", pTempDesc->size.avail);
  tjsonAddDoubleToObject(pTempdirJson, "used", pTempDesc->size.used);
  tjsonAddDoubleToObject(pTempdirJson, "total", pTempDesc->size.total);
}

static const char *monLogLevelStr(ELogLevel level) {
  if (level == DEBUG_ERROR) {
    return "error";
  } else {
    return "info";
  }
}

static void monGenLogJson(SMonInfo *pMonitor) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "log_infos", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  SMonLogs *logs[6];
  logs[0] = &pMonitor->log;
  logs[1] = &pMonitor->mmInfo.log;
  logs[2] = &pMonitor->vmInfo.log;
  logs[3] = &pMonitor->smInfo.log;
  logs[4] = &pMonitor->qmInfo.log;
  logs[5] = &pMonitor->bmInfo.log;

  int32_t numOfErrorLogs = 0;
  int32_t numOfInfoLogs = 0;
  int32_t numOfDebugLogs = 0;
  int32_t numOfTraceLogs = 0;

  for (int32_t j = 0; j < 6; j++) {
    SMonLogs *pLog = logs[j];
    numOfErrorLogs += pLog->numOfErrorLogs;
    numOfInfoLogs += pLog->numOfInfoLogs;
    numOfDebugLogs += pLog->numOfDebugLogs;
    numOfTraceLogs += pLog->numOfTraceLogs;
  }

  SJson *pSummaryJson = tjsonAddArrayToObject(pJson, "summary");
  if (pSummaryJson == NULL) return;

  SJson *pLogError = tjsonCreateObject();
  if (pLogError == NULL) return;
  tjsonAddStringToObject(pLogError, "level", "error");
  tjsonAddDoubleToObject(pLogError, "total", numOfErrorLogs);
  if (tjsonAddItemToArray(pSummaryJson, pLogError) != 0) tjsonDelete(pLogError);

  SJson *pLogInfo = tjsonCreateObject();
  if (pLogInfo == NULL) return;
  tjsonAddStringToObject(pLogInfo, "level", "info");
  tjsonAddDoubleToObject(pLogInfo, "total", numOfInfoLogs);
  if (tjsonAddItemToArray(pSummaryJson, pLogInfo) != 0) tjsonDelete(pLogInfo);

  SJson *pLogDebug = tjsonCreateObject();
  if (pLogDebug == NULL) return;
  tjsonAddStringToObject(pLogDebug, "level", "debug");
  tjsonAddDoubleToObject(pLogDebug, "total", numOfDebugLogs);
  if (tjsonAddItemToArray(pSummaryJson, pLogDebug) != 0) tjsonDelete(pLogDebug);

  SJson *pLogTrace = tjsonCreateObject();
  if (pLogTrace == NULL) return;
  tjsonAddStringToObject(pLogTrace, "level", "trace");
  tjsonAddDoubleToObject(pLogTrace, "total", numOfTraceLogs);
  if (tjsonAddItemToArray(pSummaryJson, pLogTrace) != 0) tjsonDelete(pLogTrace);
}

void monSendReport() {
  SMonInfo *pMonitor = monCreateMonitorInfo();
  if (pMonitor == NULL) return;

  monGenBasicJson(pMonitor);
  monGenClusterJson(pMonitor);
  monGenVgroupJson(pMonitor);
  monGenStbJson(pMonitor);
  monGenGrantJson(pMonitor);
  monGenDnodeJson(pMonitor);
  monGenDiskJson(pMonitor);
  monGenLogJson(pMonitor);

  char *pCont = tjsonToString(pMonitor->pJson);
  uInfoL("report cont:%s\n", pCont);
  if (pCont != NULL) {
    EHttpCompFlag flag = tsMonitor.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if (taosSendHttpReport(tsMonitor.cfg.server, tsMonUri, tsMonitor.cfg.port, pCont, strlen(pCont), flag) != 0) {
      uError("failed to send monitor msg");
    }
    taosMemoryFree(pCont);
  }

  monCleanupMonitorInfo(pMonitor);
}

void monSendPromReport() {
  char ts[50];
  sprintf(ts, "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));

  char* promStr = NULL;
  char *pCont = (char *)taos_collector_registry_bridge(TAOS_COLLECTOR_REGISTRY_DEFAULT, ts, "%" PRId64, &promStr);
  uInfoL("report cont:\n%s\n", pCont);
  uInfoL("report cont prom:\n%s\n", promStr);
  if (pCont != NULL) {
    EHttpCompFlag flag = tsMonitor.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if (taosSendHttpReport(tsMonitor.cfg.server, tsMonFwUri, tsMonitor.cfg.port, pCont, strlen(pCont), flag) != 0) {
      uError("failed to send monitor msg");
    }else{
      taos_collector_registry_clear_out(TAOS_COLLECTOR_REGISTRY_DEFAULT);
    }
    taosMemoryFreeClear(pCont);
  }
}

void monSendContent(char *pCont) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  uInfoL("report cont:\n%s\n", pCont);
  if (pCont != NULL) {
    EHttpCompFlag flag = tsMonitor.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if (taosSendHttpReport(tsMonitor.cfg.server, tsMonFwUri, tsMonitor.cfg.port, pCont, strlen(pCont), flag) != 0) {
      uError("failed to send monitor msg");
    }
  }
}