// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/ttl/db_ttl_impl.h"

#include "db/filename.h"
#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/coding.h"

namespace rocksdb {

void DBWithTTLImpl::SanitizeOptions(int32_t ttl, ColumnFamilyOptions* options,
                                    Env* env) {
  if (options->compaction_filter) {
    options->compaction_filter =
        new TtlCompactionFilter(ttl, env, options->compaction_filter);
  } else {
    options->compaction_filter_factory =
        std::shared_ptr<CompactionFilterFactory>(new TtlCompactionFilterFactory(
            ttl, env, options->compaction_filter_factory));
  }

  if (options->merge_operator) {
    options->merge_operator.reset(
        new TtlMergeOperator(options->merge_operator, env));
  }
}

// Open the db inside DBWithTTLImpl because options needs pointer to its ttl
DBWithTTLImpl::DBWithTTLImpl(DB* db) : DBWithTTL(db) {}

DBWithTTLImpl::~DBWithTTLImpl() {
  // Need to stop background compaction before getting rid of the filter
  CancelAllBackgroundWork(db_, /* wait = */ true);
  delete GetOptions().compaction_filter;
}

Status UtilityDB::OpenTtlDB(const Options& options, const std::string& dbname,
                            StackableDB** dbptr, int32_t ttl, bool read_only) {
  DBWithTTL* db;
  Status s = DBWithTTL::Open(options, dbname, &db, ttl, read_only);
  if (s.ok()) {
    *dbptr = db;
  } else {
    *dbptr = nullptr;
  }
  return s;
}

Status DBWithTTL::Open(const Options& options, const std::string& dbname,
                       DBWithTTL** dbptr, int32_t ttl, bool read_only) {

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DBWithTTL::Open(db_options, dbname, column_families, &handles,
                             dbptr, {ttl}, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DBWithTTL::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DBWithTTL** dbptr,
    std::vector<int32_t> ttls, bool read_only) {

  if (ttls.size() != column_families.size()) {
    return Status::InvalidArgument(
        "ttls size has to be the same as number of column families");
  }

  std::vector<ColumnFamilyDescriptor> column_families_sanitized =
      column_families;
  for (size_t i = 0; i < column_families_sanitized.size(); ++i) {
    DBWithTTLImpl::SanitizeOptions(
        ttls[i], &column_families_sanitized[i].options,
        db_options.env == nullptr ? Env::Default() : db_options.env);
  }
  DB* db;

  Status st;
  if (read_only) {
    st = DB::OpenForReadOnly(db_options, dbname, column_families_sanitized,
                             handles, &db);
  } else {
    st = DB::Open(db_options, dbname, column_families_sanitized, handles, &db);
  }
  if (st.ok()) {
    *dbptr = new DBWithTTLImpl(db);
  } else {
    *dbptr = nullptr;
  }
  return st;
}

Status DBWithTTLImpl::CreateColumnFamilyWithTtl(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle, int ttl) {
  ColumnFamilyOptions sanitized_options = options;
  DBWithTTLImpl::SanitizeOptions(ttl, &sanitized_options, GetEnv());

  return DBWithTTL::CreateColumnFamily(sanitized_options, column_family_name,
                                       handle);
}

Status DBWithTTLImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                         const std::string& column_family_name,
                                         ColumnFamilyHandle** handle) {
  return CreateColumnFamilyWithTtl(options, column_family_name, handle, 0);
}

Status DBWithTTLImpl::AppendMetadata(std::string* value,
                                     int32_t expiration_time,  // -1 for TTL
                                     Env* env) {
  char time_string[kTimeLength];
  char magic_number_string[kMagicNumberLength];

  // An expiration time of zero means save the current timestamp and expire
  // the record using TTLs.
  if (-1 == expiration_time_or_zero) {
    int64_t current_time;
    Status st = env->GetCurrentTime(&current_time);
    if (!st.ok()) {
      return st;
    }
    expiration_time_or_zero = (int32_t)current_time;
    val_with_metadata->append("exp:", kTimeTypeLength);
  } else {
    val_with_metadata->append("ttl:", kTimeTypeLength);
  }

  // Append the epoch time value.
  EncodeFixed32(time_string, (int32_t)expiration_time_or_zero);
  val_with_metadata->append(time_string, kTimeLength);

  // Append the magic number.
  EncodeFixed32(magic_number_string, kMagicNumber);
  val_with_metadata->append(magic_number_string, kMagicNumberLength);
  return st;
}

Status DBWithTTLImpl::AppendMetadata(const Slice& val,
                                     std::string* value,
                                     int32_t expiration_time,  // -1 for TTL
                                     Env* env) {
  val_with_metadata->reserve(val.data() + kNewMetadataLen);
  val_with_metadata->append(val.data(), val.size());
  return AppendMetadata(value, expiration_time, env);
}

Status DBWithTTLImpl::ParseMetadata(const char* value, size_t value_len,
                                   bool* is_expiration,
                                   int32_t* epoch_time,
                                   size_t* metadata_length) {
  if (value_len < kMagicNumberLength) {
    return Status::Corruption("Error: Value length less than magic number\n");
  }

  int32_t magic_number =
      DecodeFixed32(value + value_len - kMagicNumberLength);

  // If the number obtained isn't what's expected, the value is in the old
  // format, and the magic number is actually the timestamp of the write.
  if (kMagicNumber != magic_number) {
    if (is_expiration) {
      *is_expiration = true;
    }
    if (epoch_time) {
      *epoch_time = magic_number;
    }
    if (metadata_length) {
      *metadata_length = kMagicNumberLength;
    }
    return Status::OK();
  }

  if (metadata_length) {
    *metadata_length = kNewMetadataLen;
  }

  if (value_len < kNewMetadataLen) {
    return Status::Corruption("Error: Value length less than expected metadata\n");
  }

  char* time_type = value + value_len - kNewMetadataLen;
  if (epoch_time) {
    *epoch_time =
         DecodeFixed32(value + value_len - kMagicNumberLength - kTimeLength);
  }

  if (memcmp(time_type, "exp:", kTimeTypeLength) == 0) {
    if (is_expiration) {
      *is_expiration = true;
    }
    return Status::OK();
  }

  if (memcmp(time_type, "ttl:", kTimeTypeLength) == 0) {
    if (is_expiration) {
      *is_expiration = false;
    }
    return Status::OK();
  }

  return Status::Corruption("Error: Invalid time type\n");
}

Status DBWithTTLImpl::ParseMetadata(const Slice& str,
                                   bool* is_expiration,
                                   int32_t* epoch_time
                                   size_t* metadata_length) {
  return DBWithTTLImpl::ParseMetadata(str.data(), str.size(), is_expiration,
                                      epoch_time, metadata_length);
}

Status DBWithTTLImpl::ParseMetadata(const std::string& str,
                                   bool* is_expiration,
                                   int32_t* epoch_time,
                                   size_t* metadata_length) {
  return DBWithTTLImpl::ParseMetadata(str.c_str(), str.length(), is_expiration,
                                      epoch_time, metadata_length);
}

// Returns corruption if the length of the string is lesser than timestamp, or
// timestamp refers to a time lesser than ttl-feature release time
Status DBWithTTLImpl::SanityCheckMetadata(const Slice& str) {
  Status st;
  bool is_expiration;
  int32_t epoch_time;
  st = ParseMetadata(str, is_expiration, epoch_time, nullptr);
  if (!st.ok()) {
    return st;
  }

  // A non-positive expiration indicates never-expiring data.
  if (is_expiration && epoch_time <= 0) {
    return Status::OK();
  }

  // Check that TS is not lesser than kMinTimestamp.
  // Guard against corruption & normal database opened incorrectly in TTL mode.
  if (epoch_time < kMinTimestamp) {
    return Status::Corruption("Error: Time < TTL feature release time!\n");
  }
  return Status::OK();
}

// Checks if the string is stale or not according to the TTL or the expiration.
bool DBWithTTLImpl::IsStale(const Slice& value, int32_t ttl, Env* env) {
  Status st;
  bool is_expiration;
  int32_t epoch_time;
  st = ReadMetadata(str, &is_expiration, &epoch_time, nullptr);
  if (!st.ok()) {
    return false;  // On error, treat the data as fresh.
  }

  if (is_expiration) {
    // A non-positive expiration indicates never-expiring data.
    if (epoch_time <= 0) {
      return false;
    }

    int64_t curtime;
    if (!env->GetCurrentTime(&curtime).ok()) {
      return false;  // On error, treat the data as fresh.
    }
    return epoch_time < curtime;
  }

  // We're now working under the assumption the epoch_time is a timestamp whose
  // age should be compared against the TTL (if positive) specified on DB open.
  if (ttl <= 0) {
    return false;
  }

  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // On error, treat the data as fresh.
  }
  return (epoch_time + ttl) < curtime;
}

// Strips the metadata from the end of the string
Status DBWithTTLImpl::StripMetadata(std::string* str) {
  Status st;
  size_t metadata_length;
  st = ParseMetadata(str, nullptr, nullptr, &metadata_length);
  if (!st.ok()) {
    return st;
  }
  str->erase(str->length() - metadata_length, metadata_length);
  return st;
}

Status DBWithTTLImpl::PutWithExpiration(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& val, int32_t expiration_time) {
  WriteBatch batch;
  batch.Put(column_family, key, val);
  return WriteWithExpiration(options, &batch, expiration_time);
}

Status DBWithTTLImpl::Put(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& val) {
  return DBWithTTLImpl::PutWithExpiration(options, column_family, key, val, -1);
}

Status DBWithTTLImpl::Get(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          std::string* value) {
  Status st = db_->Get(options, column_family, key, value);
  if (!st.ok()) {
    return st;
  }
  st = SanityCheckTimestamp(*value);
  if (!st.ok()) {
    return st;
  }
  return StripTS(value);
}

std::vector<Status> DBWithTTLImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  auto statuses = db_->MultiGet(options, column_family, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = SanityCheckTimestamp((*values)[i]);
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = StripTS(&(*values)[i]);
  }
  return statuses;
}

bool DBWithTTLImpl::KeyMayExist(const ReadOptions& options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, std::string* value,
                                bool* value_found) {
  bool ret = db_->KeyMayExist(options, column_family, key, value, value_found);
  if (ret && value != nullptr && value_found != nullptr && *value_found) {
    if (!SanityCheckTimestamp(*value).ok() || !StripTS(value).ok()) {
      return false;
    }
  }
  return ret;
}

Status DBWithTTLImpl::MergeWithExpiration(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value, int32_t expiration_time) {
  WriteBatch batch;
  batch.Merge(column_family, key, value);
  return WriteWithExpiration(options, &batch, expiration_time);
}

Status DBWithTTLImpl::Merge(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value) {
  return DBWithTTLImpl::MergeWithExpiration(options, column_family, key, value,
                                            -1);
}

Status DBWithTTLImpl::WriteWithExpiration(const WriteOptions& opts,
                                          WriteBatch* updates,
                                          int32_t expiration_time_or_zero) {
  class Handler : public WriteBatch::Handler {
   public:
    explicit Handler(Env* env, int32_t exptime) : env_(env),expiration_time_(or_zero_expiration_time_)or_zero {}
    WriteBatch updates_ttl;
    Status batch_rewrite_status;
    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendMetadata(value, &value_with_ts,
                                 expiration_time_or_zero_, env_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendMetadata(value, &value_with_ts,
                                 expiration_time_or_zero_, env_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      updates_ttl.PutLogData(blob);
    }

   private:
    Env* env_;
    int32_t expiration_time_;or_zero_
  };
  Handler handler(GetEnv(), expiration_time)_or_zero;
  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBWithTTLImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  return DBWithTTLImpl::WriteWithExpiration(opts, updates, -1);
}

Iterator* DBWithTTLImpl::NewIterator(const ReadOptions& opts,
                                     ColumnFamilyHandle* column_family) {
  return new TtlIterator(db_->NewIterator(opts, column_family));
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
