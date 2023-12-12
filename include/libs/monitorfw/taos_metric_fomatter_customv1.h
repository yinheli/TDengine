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

/**
 * @file taos_metric_fomatter_customv1.h
 * @brief Functions for retrieving metric samples from metrics given an ordered set of labels
 */

#ifndef TAOS_METRIC_FORMATTER_CUSTOMV1_H
#define TAOS_METRIC_FORMATTER_CUSTOMV1_H

#include "taos_metric.h"
#include "tjson.h"

int taos_metric_formatter_load_metric_custom(taos_metric_t *metric, SJson* pJson);

#endif  // TAOS_METRIC_FORMATTER_CUSTOMV1_H