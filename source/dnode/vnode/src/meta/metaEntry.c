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

#include "meta.h"

int32_t metaEntryInit(const SMetaEntry *fromEntry, SMetaEntry **entry) {
  if (((*entry) = taosMemoryCalloc(1, sizeof(SMetaEntry))) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*entry)->version = fromEntry->version;
  (*entry)->type = fromEntry->type;
  (*entry)->uid = fromEntry->uid;

  if (fromEntry->type > 0) {
    if (fromEntry->name) {
      (*entry)->name = taosStrdup(fromEntry->name);
    }

    if (fromEntry->type == TSDB_SUPER_TABLE) {
      (*entry)->flags = fromEntry->flags;
      tCloneSSchemaWrapperTo(&fromEntry->stbEntry.schemaRow, &(*entry)->stbEntry.schemaRow);
      tCloneSSchemaWrapperTo(&fromEntry->stbEntry.schemaTag, &(*entry)->stbEntry.schemaTag);
      // TODO: stbEntry.rsmaParam
    } else if (fromEntry->type == TSDB_CHILD_TABLE) {
      (*entry)->ctbEntry.btime = fromEntry->ctbEntry.btime;
      (*entry)->ctbEntry.ttlDays = fromEntry->ctbEntry.ttlDays;
      (*entry)->ctbEntry.commentLen = fromEntry->ctbEntry.commentLen;
      if (fromEntry->ctbEntry.commentLen > 0) {
        (*entry)->ctbEntry.comment = taosStrdup(fromEntry->ctbEntry.comment);
      }
      (*entry)->ctbEntry.suid = fromEntry->ctbEntry.suid;
      // TODO: tTagDup(&fromEntry->ctbEntry.pTags, &(*entry)->ctbEntry.pTags);
    } else if (fromEntry->type == TSDB_NORMAL_TABLE) {
      (*entry)->ntbEntry.btime = fromEntry->ntbEntry.btime;
      (*entry)->ntbEntry.ttlDays = fromEntry->ntbEntry.ttlDays;
      (*entry)->ntbEntry.commentLen = fromEntry->ntbEntry.commentLen;
      if (fromEntry->ntbEntry.commentLen) {
        (*entry)->ntbEntry.comment = taosStrdup(fromEntry->ntbEntry.comment);
      }
      (*entry)->ntbEntry.ncid = fromEntry->ntbEntry.ncid;
      tCloneSSchemaWrapperTo(&fromEntry->ntbEntry.schemaRow, &(*entry)->ntbEntry.schemaRow);
    } else if (fromEntry->type == TSDB_TSMA_TABLE) {
      // TODO
    } else {
      ASSERT(0);
    }
  }

  return 0;
}

int32_t metaEntryDestroy(SMetaEntry *entry) {
  if (entry == NULL) return 0;

  if (entry->type > 0) {  // TODO
    taosMemoryFree(entry->name);
  }
  taosMemoryFree(entry);
  return 0;
}

int metaEncodeEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI64(pCoder, pME->version) < 0) return -1;
  if (tEncodeI8(pCoder, pME->type) < 0) return -1;
  if (tEncodeI64(pCoder, pME->uid) < 0) return -1;
  if (pME->type > 0) {
    if (pME->name == NULL || tEncodeCStr(pCoder, pME->name) < 0) return -1;

    if (pME->type == TSDB_SUPER_TABLE) {
      if (tEncodeI8(pCoder, pME->flags) < 0) return -1;  // TODO: need refactor?
      if (tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaRow) < 0) return -1;
      if (tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaTag) < 0) return -1;
      if (TABLE_IS_ROLLUP(pME->flags)) {
        if (tEncodeSRSmaParam(pCoder, &pME->stbEntry.rsmaParam) < 0) return -1;
      }
    } else if (pME->type == TSDB_CHILD_TABLE) {
      if (tEncodeI64(pCoder, pME->ctbEntry.btime) < 0) return -1;
      if (tEncodeI32(pCoder, pME->ctbEntry.ttlDays) < 0) return -1;
      if (tEncodeI32v(pCoder, pME->ctbEntry.commentLen) < 0) return -1;
      if (pME->ctbEntry.commentLen > 0) {
        if (tEncodeCStr(pCoder, pME->ctbEntry.comment) < 0) return -1;
      }
      if (tEncodeI64(pCoder, pME->ctbEntry.suid) < 0) return -1;
      if (tEncodeTag(pCoder, (const STag *)pME->ctbEntry.pTags) < 0) return -1;
    } else if (pME->type == TSDB_NORMAL_TABLE) {
      if (tEncodeI64(pCoder, pME->ntbEntry.btime) < 0) return -1;
      if (tEncodeI32(pCoder, pME->ntbEntry.ttlDays) < 0) return -1;
      if (tEncodeI32v(pCoder, pME->ntbEntry.commentLen) < 0) return -1;
      if (pME->ntbEntry.commentLen > 0) {
        if (tEncodeCStr(pCoder, pME->ntbEntry.comment) < 0) return -1;
      }
      if (tEncodeI32v(pCoder, pME->ntbEntry.ncid) < 0) return -1;
      if (tEncodeSSchemaWrapper(pCoder, &pME->ntbEntry.schemaRow) < 0) return -1;
    } else if (pME->type == TSDB_TSMA_TABLE) {
      if (tEncodeTSma(pCoder, pME->smaEntry.tsma) < 0) return -1;
    } else {
      metaError("meta/entry: invalide table type: %" PRId8 " encode failed.", pME->type);

      return -1;
    }
  }

  tEndEncode(pCoder);
  return 0;
}

int metaDecodeEntry(SDecoder *pCoder, SMetaEntry *pME) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI64(pCoder, &pME->version) < 0) return -1;
  if (tDecodeI8(pCoder, &pME->type) < 0) return -1;
  if (tDecodeI64(pCoder, &pME->uid) < 0) return -1;
  if (pME->type > 0) {
    if (tDecodeCStr(pCoder, &pME->name) < 0) return -1;

    if (pME->type == TSDB_SUPER_TABLE) {
      if (tDecodeI8(pCoder, &pME->flags) < 0) return -1;  // TODO: need refactor?
      if (tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaRow) < 0) return -1;
      if (tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaTag) < 0) return -1;
      if (TABLE_IS_ROLLUP(pME->flags)) {
        if (tDecodeSRSmaParam(pCoder, &pME->stbEntry.rsmaParam) < 0) return -1;
      }
    } else if (pME->type == TSDB_CHILD_TABLE) {
      if (tDecodeI64(pCoder, &pME->ctbEntry.btime) < 0) return -1;
      if (tDecodeI32(pCoder, &pME->ctbEntry.ttlDays) < 0) return -1;
      if (tDecodeI32v(pCoder, &pME->ctbEntry.commentLen) < 0) return -1;
      if (pME->ctbEntry.commentLen > 0) {
        if (tDecodeCStr(pCoder, &pME->ctbEntry.comment) < 0) return -1;
      }
      if (tDecodeI64(pCoder, &pME->ctbEntry.suid) < 0) return -1;
      if (tDecodeTag(pCoder, (STag **)&pME->ctbEntry.pTags) < 0) return -1;  // (TODO)
    } else if (pME->type == TSDB_NORMAL_TABLE) {
      if (tDecodeI64(pCoder, &pME->ntbEntry.btime) < 0) return -1;
      if (tDecodeI32(pCoder, &pME->ntbEntry.ttlDays) < 0) return -1;
      if (tDecodeI32v(pCoder, &pME->ntbEntry.commentLen) < 0) return -1;
      if (pME->ntbEntry.commentLen > 0) {
        if (tDecodeCStr(pCoder, &pME->ntbEntry.comment) < 0) return -1;
      }
      if (tDecodeI32v(pCoder, &pME->ntbEntry.ncid) < 0) return -1;
      if (tDecodeSSchemaWrapperEx(pCoder, &pME->ntbEntry.schemaRow) < 0) return -1;
    } else if (pME->type == TSDB_TSMA_TABLE) {
      pME->smaEntry.tsma = tDecoderMalloc(pCoder, sizeof(STSma));
      if (!pME->smaEntry.tsma) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
      if (tDecodeTSma(pCoder, pME->smaEntry.tsma, true) < 0) return -1;
    } else {
      metaError("meta/entry: invalide table type: %" PRId8 " decode failed.", pME->type);

      return -1;
    }
  }

  tEndDecode(pCoder);
  return 0;
}

static void metaBuildEntryInfo(const SMetaEntry *entry, SMetaInfo *info) {
  info->uid = entry->uid;
  info->version = entry->version;
  if (entry->type == TSDB_SUPER_TABLE) {
    info->suid = entry->uid;
    info->skmVer = entry->stbEntry.schemaRow.version;
  } else if (entry->type == TSDB_CHILD_TABLE) {
    info->suid = entry->ctbEntry.suid;
    info->skmVer = 0;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    info->suid = 0;
    info->skmVer = entry->ntbEntry.schemaRow.version;
  } else {
    metaError("meta/table: invalide table type: %" PRId8 " get entry info failed.", entry->type);
  }
}

// entry index ========
static int32_t metaUpsertTableEntry(SMeta *meta, const SMetaEntry *entry) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SEncoder encoder = {0};
  void    *value = NULL;
  int32_t  valueSize = 0;

  // encode
  tEncodeSize(metaEncodeEntry, entry, valueSize, code);
  if (code < 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }

  if ((value = taosMemoryMalloc(valueSize)) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  tEncoderInit(&encoder, value, valueSize);
  if (metaEncodeEntry(&encoder, entry) != 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }
  tEncoderClear(&encoder);

  // insert
  if (tdbTbInsert(meta->pTbDb,
                  &(STbDbKey){
                      .version = entry->version,
                      .uid = entry->uid,
                  },
                  sizeof(STbDbKey), value, valueSize, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  taosMemoryFree(value);
  return (terrno = code);
}

static int32_t metaDropTableEntry(SMeta *meta, const SMetaEntry *entry) {
  STbDbKey key = {
      .version = entry->version,
      .uid = entry->uid,
  };
  if (tdbTbDelete(meta->pTbDb, &key, sizeof(key), meta->txn) != 0) {
    return TSDB_CODE_TDB_OP_ERROR;
  }
  return 0;
}

// schema index ========
static int32_t metaUpsertSchemaIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t  code = 0;
  int32_t  lino = 0;
  void    *value = NULL;
  int32_t  valueSize = 0;
  SEncoder encoder = {0};

  const SSchemaWrapper *schema = NULL;
  if (entry->type == TSDB_SUPER_TABLE) {
    schema = &entry->stbEntry.schemaRow;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    schema = &entry->ntbEntry.schemaRow;
  }

  ASSERT(schema != NULL);

  // encode
  tEncodeSize(tEncodeSSchemaWrapper, schema, valueSize, code);
  if (code < 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }
  if ((value = taosMemoryMalloc(valueSize)) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  tEncoderInit(&encoder, value, valueSize);
  if (tEncodeSSchemaWrapper(&encoder, schema) != 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }
  tEncoderClear(&encoder);

  // do insert
  SSkmDbKey key = {
      .sver = schema->version,
      .uid = entry->uid,
  };
  if (tdbTbInsert(meta->pSkmDb, &key, sizeof(key), value, valueSize, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  taosMemoryFree(value);
  return code;
}

static int32_t metaDropSchemaIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *schema = NULL;
  if (entry->type == TSDB_SUPER_TABLE) {
    schema = &entry->stbEntry.schemaRow;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    schema = &entry->ntbEntry.schemaRow;
  }

  ASSERT(schema != NULL);

  // drop
  SSkmDbKey key = {
      .sver = schema->version,
      .uid = entry->uid,
  };
  if (tdbTbDelete(meta->pSkmDb, &key, sizeof(key), meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// uid index ========
static int32_t metaUpsertUidIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMetaInfo info;

  metaBuildEntryInfo(entry, &info);

  code = metaCacheUpsert(meta, &info);
  TSDB_CHECK_CODE(code, lino, _exit);

  SUidIdxVal uidIdxVal = {
      .suid = info.suid,
      .version = info.version,
      .skmVer = info.skmVer,
  };

  if (tdbTbUpsert(meta->pUidIdx, &entry->uid, sizeof(tb_uid_t), &uidIdxVal, sizeof(uidIdxVal), meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropUidIdx(SMeta *meta, const SMetaEntry *entry) {
  metaCacheDrop(meta, entry->uid);
  if (tdbTbDelete(meta->pUidIdx, &entry->uid, sizeof(entry->uid), meta->txn) != 0) {
    return TSDB_CODE_TDB_OP_ERROR;
  }
  return 0;
}

// suid index ========
static int32_t metaUpsertSuidIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(entry->type == TSDB_SUPER_TABLE);

  if (tdbTbUpsert(meta->pSuidIdx, &entry->uid, sizeof(tb_uid_t), NULL, 0, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropSuidIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(entry->type == TSDB_SUPER_TABLE);

  if (tdbTbDelete(meta->pSuidIdx, &entry->uid, sizeof(entry->uid), meta->txn) != 0) {
    return TSDB_CODE_TDB_OP_ERROR;
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// name index ========
static int32_t metaUpsertNameIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tdbTbUpsert(meta->pNameIdx, entry->name, strlen(entry->name) + 1, &entry->uid, sizeof(tb_uid_t), meta->txn) !=
      0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropNameIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tdbTbDelete(meta->pNameIdx, entry->name, strlen(entry->name) + 1, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// cid index ========
static int32_t metaUpsertCtbIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(entry->type == TSDB_CHILD_TABLE);

  SCtbIdxKey ctbIdxKey = {
      .suid = entry->ctbEntry.suid,
      .uid = entry->uid,
  };

  STag *tags = (STag *)entry->ctbEntry.pTags;
  if (tdbTbUpsert(meta->pCtbIdx, &ctbIdxKey, sizeof(ctbIdxKey), tags, tags->len, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropCtbIdx(SMeta *meta, const SMetaEntry *entry) {
  ASSERT(entry->type == TSDB_CHILD_TABLE);

  SCtbIdxKey ctbIdxKey = {
      .suid = entry->ctbEntry.suid,
      .uid = entry->uid,
  };
  if (tdbTbDelete(meta->pCtbIdx, &ctbIdxKey, sizeof(ctbIdxKey), meta->txn) != 0) {
    return TSDB_CODE_TDB_OP_ERROR;
  }
  return 0;
}

static int32_t metaGetChildTableUidLists(SMeta *meta, int64_t suid, SArray *uidArray) {
  int32_t code = 0;
  int32_t lino = 0;
  TBC    *childTableIndexCursor = NULL;
  void   *key = NULL;
  void   *value = NULL;
  int32_t keySize = 0;
  int32_t valueSize = 0;

  taosArrayClear(uidArray);

  code = tdbTbcOpen(meta->pCtbIdx, &childTableIndexCursor, NULL);
  TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);

  code = tdbTbcMoveTo(childTableIndexCursor, &(SCtbIdxKey){.suid = suid, .uid = INT64_MIN}, sizeof(SCtbIdxKey), &lino);
  TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);

  for (;;) {
    if (tdbTbcNext(childTableIndexCursor, &key, &keySize, &value, &valueSize) != 0) {
      break;
    }

    SCtbIdxKey *ctbIdxKey = (SCtbIdxKey *)key;
    if (ctbIdxKey->suid < suid) {
      continue;
    } else if (ctbIdxKey->suid > suid) {
      break;
    }

    taosArrayPush(uidArray, &ctbIdxKey->uid);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbTbcClose(childTableIndexCursor);
  tdbFree(key);
  tdbFree(value);
  return code;
}

static int32_t metaGetTableEntryImpl(SMeta *meta, int64_t uid, SMetaEntry **entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  // search uid.idx
  if (tdbTbGet(meta->pUidIdx, &uid, sizeof(uid), &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _exit);
  }

  // search table.db
  STbDbKey tbDbKey = {
      .uid = uid,
      .version = ((SUidIdxVal *)value)->version,
  };
  if (tdbTbGet(meta->pTbDb, &tbDbKey, sizeof(tbDbKey), &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _exit);
  }

  // decode the entry
  SDecoder   decoder = {0};
  SMetaEntry decodeEntry = {0};

  tDecoderInit(&decoder, value, valueSize);
  if (metaDecodeEntry(&decoder, &decodeEntry) != 0) {
    tDecoderClear(&decoder);
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_DATA_FMT, lino, _exit);
  }
  tDecoderClear(&decoder);

  // make a copy of the entry
  code = metaEntryInit(&decodeEntry, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbFree(value);
  return (terrno = code);
}

static int32_t metaDeleteChildTable(SMeta *meta, const SMetaEntry *childEntry, SMetaEntry *superEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *superEntryInUse = superEntry;

  if (superEntryInUse == NULL) {
    code = metaGetTableEntryImpl(meta, childEntry->ctbEntry.suid, &superEntryInUse);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // TODO: drop tags

  /* name.idx */
  code = metaDropNameIdx(meta, childEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* ctb.idx */
  code = metaDropCtbIdx(meta, childEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* uid.idx */
  code = metaDropUidIdx(meta, childEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  // other operations
  // metaDeleteTtl(meta, childEntry);
  // metaDeleteBtimeIdx(meta, childEntry);
  meta->pVnode->config.vndStats.numOfCTables--;
  metaUpdateStbStats(meta, childEntry->ctbEntry.suid, -1, 0);
  metaUidCacheClear(meta, childEntry->ctbEntry.suid);
  metaTbGroupCacheClear(meta, childEntry->ctbEntry.suid);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s: child table name:%s child table uid:%" PRId64
              " child table version:%" PRId64,
              TD_VID(meta->pVnode), __func__, lino, tstrerror(code), childEntry->name, childEntry->uid,
              childEntry->version);
  } else {
    metaDebug("vgId:%d drop child table entry:super table name:%s super table uid:%" PRId64
              " super table version:%" PRId64 " child table name:%s child table uid:%" PRId64
              " child table version:%" PRId64,
              TD_VID(meta->pVnode), superEntryInUse->name, superEntryInUse->uid, superEntryInUse->version,
              childEntry->name, childEntry->uid, childEntry->version);
  }
  if (superEntry == NULL) {
    metaEntryDestroy(superEntryInUse);
  }
  return (terrno = code);
}

static int32_t metaDeleteSuperTable(SMeta *meta, SMetaEntry *superEntry) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray *uidArray;

  if ((uidArray = taosArrayInit(512, sizeof(tb_uid_t))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  code = metaGetChildTableUidLists(meta, superEntry->uid, uidArray);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t i = 0; i < taosArrayGetSize(uidArray); i++) {
    int64_t     uid = *(int64_t *)taosArrayGet(uidArray, i);
    SMetaEntry *childTableEntry = NULL;

    code = metaGetTableEntryImpl(meta, uid, &childTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = metaDeleteChildTable(meta, childTableEntry, superEntry);
    if (code) {
      metaEntryDestroy(childTableEntry);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    metaEntryDestroy(childTableEntry);
  }

  /* uid.idx */
  code = metaDropUidIdx(meta, superEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* name.idx */
  code = metaDropNameIdx(meta, superEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* suid.idx */
  code = metaDropSuidIdx(meta, superEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  metaStatsCacheDrop(meta, superEntry->uid);
#if 0
  (void)tsdbCacheDropSubTables(pMeta->pVnode->pTsdb, tbUidList, pReq->suid); // TODO: take dead lock into account
  metaUpdTimeSeriesNum(meta);
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  taosArrayDestroy(uidArray);
  return code;
}

static int32_t metaDeleteNormalTable(SMeta *meta, const SMetaEntry *normalEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* name.idx */
  code = metaDropNameIdx(meta, normalEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* uid.idx */
  code = metaDropUidIdx(meta, normalEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  // TODO
  // metaDeleteBtimeIdx(meta, normalEntry);
  // tsdbCacheDropTable(pMeta->pVnode->pTsdb, e.uid, -1, &e.ntbEntry.schemaRow);
  // metaDeleteNcolIdx(meta, normalEntry);
  // metaDeleteTtl(meta, normalEntry);

  meta->pVnode->config.vndStats.numOfNTables--;
  meta->pVnode->config.vndStats.numOfNTimeSeries -= (normalEntry->ntbEntry.schemaRow.nCols - 1);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleEntryDelete(SMeta *meta, const SMetaEntry *entry) {
  int32_t     code = 0;
  int32_t     lino = 0;
  int8_t      type = -entry->type;
  SMetaEntry *savedEntry = NULL;

  code = metaGetTableEntryImpl(meta, entry->uid, &savedEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(savedEntry->type == type && savedEntry->version < entry->version);

  if (type == TSDB_SUPER_TABLE) {
    code = metaDeleteSuperTable(meta, savedEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (type == TSDB_CHILD_TABLE) {
    code = metaDeleteChildTable(meta, savedEntry, NULL);
  } else if (type == TSDB_NORMAL_TABLE) {
    code = metaDeleteNormalTable(meta, savedEntry);
  } else {
    ASSERT(0);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryDestroy(savedEntry);
  return code;
}

static int32_t metaInsertSuperTable(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* schema.db */
  code = metaUpsertSchemaIdx(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* suid.idx */
  code = metaUpsertSuidIdx(meta, entry);
  VND_CHECK_CODE(code, lino, _exit);

  meta->pVnode->config.vndStats.numOfSTables++;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return (terrno = code);
}

static int32_t metaInsertChildTable(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* cid.idx */
  code = metaUpsertCtbIdx(meta, entry);
  VND_CHECK_CODE(code, lino, _exit);

  /* cid.idx */
  // code = metaUpdateTagIdx(meta, entry);
  VND_CHECK_CODE(code, lino, _exit);

  meta->pVnode->config.vndStats.numOfCTables++;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return (terrno = code);
}

static int32_t metaInsertNormalTable(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* schema.db */
  code = metaUpsertSchemaIdx(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  meta->pVnode->config.vndStats.numOfNTables++;
  meta->pVnode->config.vndStats.numOfNTimeSeries += (entry->ntbEntry.schemaRow.nCols - 1);

  // metaUpdateNcolIdx(meta, entry);
  // metaTimeSeriesNotifyCheck(meta);
  // if (!TSDB_CACHE_NO(meta->pVnode->config)) {
  //   tsdbCacheNewTable(meta->pVnode->pTsdb, me.uid, -1, &me.ntbEntry.schemaRow);
  // }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return (terrno = code);
}

static int32_t metaHandleEntryInsert(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* name.idx */
  code = metaUpsertNameIdx(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (entry->type == TSDB_SUPER_TABLE) {
    code = metaInsertSuperTable(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (entry->type == TSDB_CHILD_TABLE) {
    code = metaInsertChildTable(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    code = metaInsertNormalTable(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(0);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return (terrno = code);
}

static int32_t metaUpdateSuperTable(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  return (terrno = code);
}

static int32_t metaUpdateChildTable(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  return (terrno = code);
}

static int32_t metaUpdateNormalTable(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  return (terrno = code);
}

static int32_t metaHandleEntryUpdate(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (entry->type == TSDB_SUPER_TABLE) {
    code = metaUpdateSuperTable(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (entry->type == TSDB_CHILD_TABLE) {
    code = metaUpdateChildTable(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    code = metaUpdateNormalTable(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(0);
  }

_exit:
  return (terrno = code);
}

int32_t metaHandleEntry(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;
  char   *name;

  metaWLock(meta);

  /* table.db */
  code = metaUpsertTableEntry(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (entry->type < 0) {
    name = NULL;
    code = metaHandleEntryDelete(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    bool update = false;

    name = entry->name;

    if (tdbTbGet(meta->pUidIdx, &entry->uid, sizeof(entry->uid), NULL, NULL) == 0) {
      update = true;
    }

    /* uid.idx */
    code = metaUpsertUidIdx(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);

    // update or insert
    if (update) {
      code = metaHandleEntryUpdate(meta, entry);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = metaHandleEntryInsert(meta, entry);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // metaUpdateBtimeIdx(meta, entry);
    if (entry->type != TSDB_SUPER_TABLE) {
      // metaUpdateTtl(meta, entry);
    }
  }

  meta->changed = true;

_exit:
  metaULock(meta);
  if (code) {
    metaError("vgId:%d, handle meta entry failed at line %d since %s, type:%d ver:%" PRId64 ", uid:%" PRId64
              ", name:%s",
              TD_VID(meta->pVnode), lino, tstrerror(code), entry->type, entry->version, entry->uid, name);
  } else {
    metaDebug("vgId:%d, handle meta entry succeed, type:%d version:%" PRId64 ", uid:%" PRId64 ", name:%s",
              TD_VID(meta->pVnode), entry->type, entry->version, entry->uid, name);
  }
  return (terrno = code);
}
