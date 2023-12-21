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
#include "monitor.h"
#include "monInt.h"

#include "thash.h"
#include "taos_monitor.h"

extern SMonitor tsMonitor;

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

void monInitNewMonitor(){
  tsMonitor.metrics = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  taos_gauge_t *gauge = NULL;

  int32_t label_count =1;
  const char *sample_labels[] = {"clusterid"};
  char *metric[] = {MASTER_UPTIME, DBS_TOTAL, TBS_TOTAL, STBS_TOTAL, VGROUPS_TOTAL,
                VGROUPS_ALIVE, VNODES_TOTAL, VNODES_ALIVE, CONNECTIONS_TOTAL, TOPICS_TOTAL, STREAMS_TOTAL,
                    DNODES_TOTAL, DNODES_ALIVE};
  for(int32_t i = 0; i < 13; i++){
    gauge= taos_gauge_new(metric[i], "",  label_count, sample_labels);
    if(taos_collector_registry_register_metric(gauge) == 1){
      taos_counter_destroy(gauge);
    }
    taosHashPut(tsMonitor.metrics, metric[i], strlen(metric[i]), &gauge, sizeof(taos_gauge_t *));
  } 

  int32_t vgroup_label_count = 3;
  const char *vgroup_sample_labels[] = {"clusterid", "vgroup_id", "database_name"};
  char *vgroup_metrics[] = {TABLES_NUM, STATUS};
  for(int32_t i = 0; i < 2; i++){
    gauge= taos_gauge_new(vgroup_metrics[i], "",  vgroup_label_count, vgroup_sample_labels);
    if(taos_collector_registry_register_metric(gauge) == 1){
      taos_counter_destroy(gauge);
    }
    taosHashPut(tsMonitor.metrics, vgroup_metrics[i], strlen(vgroup_metrics[i]), &gauge, sizeof(taos_gauge_t *));
  }

  int32_t dnodes_label_count = 3;
  const char *dnodes_sample_labels[] = {"clusterid", "dnode_id", "dnode_ep"};
  char *dnodes_gauges[] = {UPTIME, CPU_ENGINE, CPU_SYSTEM, MEM_ENGINE, MEM_SYSTEM, DISK_ENGINE, DISK_USED, NET_IN,
                            NET_OUT, IO_READ, IO_WRITE, IO_READ_DISK, IO_WRITE_DISK, ERRORS,
                             VNODES_NUM, MASTERS, HAS_MNODE, HAS_QNODE, HAS_SNODE, DNODE_STATUS};
  for(int32_t i = 0; i < 20; i++){
    gauge= taos_gauge_new(dnodes_gauges[i], "",  dnodes_label_count, dnodes_sample_labels);
    if(taos_collector_registry_register_metric(gauge) == 1){
      taos_counter_destroy(gauge);
    }
    taosHashPut(tsMonitor.metrics, dnodes_gauges[i], strlen(dnodes_gauges[i]), &gauge, sizeof(taos_gauge_t *));
  }
}

void monGenClusterInfoTable(SMonBasicInfo *pBasicInfo, SMonClusterInfo *pInfo){
  if(pBasicInfo->cluster_id == 0) return;
  char buf[50] = {0};
  snprintf(buf, sizeof(buf), "%" PRId64, pBasicInfo->cluster_id);
  const char *sample_labels[] = {buf};

  taos_gauge_t **metric = NULL;
  
  metric = taosHashGet(tsMonitor.metrics, MASTER_UPTIME, strlen(MASTER_UPTIME));
  taos_gauge_set(*metric, pInfo->master_uptime, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, DBS_TOTAL, strlen(DBS_TOTAL));
  taos_gauge_set(*metric, pInfo->dbs_total, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, TBS_TOTAL, strlen(TBS_TOTAL));
  taos_gauge_set(*metric, pInfo->tbs_total, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, STBS_TOTAL, strlen(STBS_TOTAL));
  taos_gauge_set(*metric, pInfo->stbs_total, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, VGROUPS_TOTAL, strlen(VGROUPS_TOTAL));
  taos_gauge_set(*metric, pInfo->vgroups_total, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, VGROUPS_ALIVE, strlen(VGROUPS_ALIVE));
  taos_gauge_set(*metric, pInfo->vgroups_alive, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, VNODES_TOTAL, strlen(VNODES_TOTAL));
  taos_gauge_set(*metric, pInfo->vnodes_total, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, VNODES_ALIVE, strlen(VNODES_ALIVE));
  taos_gauge_set(*metric, pInfo->vnodes_alive, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, CONNECTIONS_TOTAL, strlen(CONNECTIONS_TOTAL));
  taos_gauge_set(*metric, pInfo->vnodes_alive, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, TOPICS_TOTAL, strlen(TOPICS_TOTAL));
  taos_gauge_set(*metric, pInfo->topics_toal, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, STREAMS_TOTAL, strlen(STREAMS_TOTAL));
  taos_gauge_set(*metric, pInfo->streams_total, sample_labels);

  int32_t dnode_total = taosArrayGetSize(pInfo->dnodes);
  int32_t dnode_alive = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->dnodes); ++i) {
    SMonDnodeDesc *pDnodeDesc = taosArrayGet(pInfo->dnodes, i);

    if(strcmp(pDnodeDesc->status, "ready") == 0){
        dnode_alive++;
    }
  }
    
  metric = taosHashGet(tsMonitor.metrics, DNODES_TOTAL, strlen(DNODES_TOTAL));
  taos_gauge_set(*metric, dnode_total, sample_labels);

  metric = taosHashGet(tsMonitor.metrics, DNODES_ALIVE, strlen(DNODES_ALIVE));
  taos_gauge_set(*metric, dnode_alive, sample_labels);
}