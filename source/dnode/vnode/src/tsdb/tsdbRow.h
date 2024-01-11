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

#include "tsdb.h"

#ifndef __TSDB_ROW_H__
#define __TSDB_ROW_H__

#ifdef __cplusplus
extern "C" {
#endif

struct STRowKey {
  SRowKey rowKey;
  int64_t version;
};

void    tsdbRowGetKey(TSDBROW *row, STRowKey *rowKey);
int32_t tTRowKeyCmpr(const void *p1, const void *p2);

#ifdef __cplusplus
}
#endif

#endif /*__TSDB_ROW_H__*/