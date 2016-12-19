// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/utility_db.h"
#include "rocksdb/utilities/db_ttl.h"
#include "db/db_impl.h"

#ifdef _WIN32
// Windows API macro interference
#undef GetCurrentTime
#endif


namespace rocksdb {

	class DBWithTTLImpl : public DBWithTTL {
 public:
  static void SanitizeOptions(int32_t ttl, ColumnFamilyOptions* options,
                              Env* env);

  explicit DBWithTTLImpl(DB* db);

  virtual ~DBWithTTLImpl();

  Status CreateColumnFamilyWithTtl(const ColumnFamilyOptions& options,
                                   const std::string& column_family_name,
                                   ColumnFamilyHandle** handle,
                                   int ttl) override;

  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const std::string& column_family_name,
                            ColumnFamilyHandle** handle) override;

  Status PutWithExpiration(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val, int32_t exptime);

  using StackableDB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val) override;

  using StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) override;

  using StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  using StackableDB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override;

  Status MergeWithExpiration(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value, int32_t exptime);

  using StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;

  Status WriteWithExpiration(const WriteOptions& opts, WriteBatch* updates,
			     int32_t expiration_time);
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  using StackableDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& opts,
                                ColumnFamilyHandle* column_family) override;

  virtual DB* GetBaseDB() override { return db_; }

  static bool IsStale(const Slice& value, int32_t ttl, Env* env);

  static Status AppendMetadata(std::string& value, int32_t expiration_time,
                               Env* env);
  static Status AppendMetadata(const Slice& val, std::string& value,
                               int32_t expiration_time, Env* env);


  static Status ParseMetadata(const char* value, size_t value_len,
                              bool* is_expiration, int32_t* epoch_time,
			                        size_t* length);
  static Status ParseMetadata(const Slice& str, bool* is_expiration,
                              int32_t* epoch_time, size_t* metadata_length);
  static Status ParseMetadata(const std::string& str, bool* is_expiration,
                              int32_t* epoch_time, size_t* length);

  static Status SanityCheckMetadata(const Slice& str);

  static Status StripMetadata(std::string* str);

  // The type includes three ASCII characters for type plus a colon delimiter.
  // Currently supported values are "exp:" and "ttl:".
  static const uint32_t kTimeTypeLength = sizeof(char) * 4;

  static const uint32_t kTimeLength = sizeof(int32_t);

  // The magic number uses the epoch time's string length to guarantee
  // compatibility with previous releases that only appended a timestamp. This
  // magic number is outside the valid range of allowed timestamps, so it is a
  // safe indicator that the value is in the new format.
  static const uint32_t kMagicNumberLength = kTimeLength;

  static const size_t kNewMetadataLen = kTimeTypeLength + kTimeLength + kMagicNumberLength;

  static const int32_t kMagicNumber = 7904202;

  static const int32_t kMinTimestamp = 1368146402;  // 05/09/2013:5:40PM GMT-8

  static const int32_t kMaxTimestamp = 2147483647;  // 01/18/2038:7:14PM GMT-8
};

class TtlIterator : public Iterator {

 public:
  explicit TtlIterator(Iterator* iter) : iter_(iter) { assert(iter_); }

  ~TtlIterator() { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }

  void SeekToFirst() override { iter_->SeekToFirst(); }

  void SeekToLast() override { iter_->SeekToLast(); }

  void Seek(const Slice& target) override { iter_->Seek(target); }

  void SeekForPrev(const Slice& target) override { iter_->SeekForPrev(target); }

  void Next() override { iter_->Next(); }

  void Prev() override { iter_->Prev(); }

  Slice key() const override { return iter_->key(); }

  int32_t timestamp() const {
    int32_t epoch_time = 0;
    assert(DBWithTTLImpl::ParseMetadata(iter_->value(), nullptr, &epoch_time,
                                        nullptr).ok());
    return epoch_time;
  }

  Slice value() const override {
    // TODO: handle metadata corruption like in general iterator semantics
    assert(DBWithTTLImpl::SanityCheckMetadata(iter_->value()).ok());
    Slice trimmed_value = iter_->value();
    Status st;
    size_t metadata_length = 0;
    assert(DBWithTTLImpl::ParseMetadata(trimmed_value, nullptr, nullptr,
					&metadata_length).ok());
    trimmed_value.size_ -= metadata_length;
    return trimmed_value;
  }

  Status status() const override { return iter_->status(); }

 private:
  Iterator* iter_;
};

class TtlCompactionFilter : public CompactionFilter {
 public:
  TtlCompactionFilter(
      int32_t ttl, Env* env, const CompactionFilter* user_comp_filter,
      std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory =
          nullptr)
      : ttl_(ttl),
        env_(env),
        user_comp_filter_(user_comp_filter),
        user_comp_filter_from_factory_(
            std::move(user_comp_filter_from_factory)) {
    // Unlike the merge operator, compaction filter is necessary for TTL, hence
    // this would be called even if user doesn't specify any compaction-filter
    if (!user_comp_filter_) {
      user_comp_filter_ = user_comp_filter_from_factory_.get();
    }
  }

  virtual bool Filter(int level, const Slice& key, const Slice& old_val,
                      std::string* new_val, bool* value_changed) const
      override {
    if (DBWithTTLImpl::IsStale(old_val, ttl_, env_)) {
      return true;
    }
    if (user_comp_filter_ == nullptr) {
      return false;
    }
    size_t metadata_length = 0;
    assert(DBWithTTLImpl::ParseMetadata(old_val, nullptr, nullptr,
					&metadata_length).ok());
    Slice old_val_without_metadata(old_val.data(),
                                   old_val.size() - metadata_length);
    if (user_comp_filter_->Filter(level, key, old_val_without_metadata, new_val,
                                  value_changed)) {
      return true;
    }
    if (*value_changed) {
      new_val->append(
          old_val.data() + old_val.size() - metadata_length, metadata_length);
    }
    return false;
  }

  virtual const char* Name() const override { return "Delete By TTL"; }

 private:
  int32_t ttl_;
  Env* env_;
  const CompactionFilter* user_comp_filter_;
  std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory_;
};

class TtlCompactionFilterFactory : public CompactionFilterFactory {
 public:
  TtlCompactionFilterFactory(
      int32_t ttl, Env* env,
      std::shared_ptr<CompactionFilterFactory> comp_filter_factory)
      : ttl_(ttl), env_(env), user_comp_filter_factory_(comp_filter_factory) {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory =
        nullptr;
    if (user_comp_filter_factory_) {
      user_comp_filter_from_factory =
          user_comp_filter_factory_->CreateCompactionFilter(context);
    }

    return std::unique_ptr<TtlCompactionFilter>(new TtlCompactionFilter(
        ttl_, env_, nullptr, std::move(user_comp_filter_from_factory)));
  }

  virtual const char* Name() const override {
    return "TtlCompactionFilterFactory";
  }

 private:
  int32_t ttl_;
  Env* env_;
  std::shared_ptr<CompactionFilterFactory> user_comp_filter_factory_;
};

class TtlMergeOperator : public MergeOperator {

 public:
  explicit TtlMergeOperator(const std::shared_ptr<MergeOperator>& merge_op,
                            Env* env)
      : user_merge_op_(merge_op), env_(env) {
    assert(merge_op);
    assert(env);
  }

  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    Status st;
    size_t existing_value_metadata_len;
    bool existing_is_expiration = false;
    int32_t existing_epoch_time = -1;

    // Obtain the metadata length for the existing value, if any.
    // This also verifies that it's present and valid.
    if (merge_in.existing_value) {
      st = DBWithTTLImpl::ParseMetadata(*merge_in.existing_value, nullptr,
                                        nullptr, &existing_value_metadata_len);
      if (!st.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, merge_in.logger,
            "Error: Invalid metadata on existing value.");
        return false;
      }
    }

    // Strip metadata from each operand to be passed to user_merge_op_
    std::vector<Slice> operands_without_metadata;
    for (const auto& operand : merge_in.operand_list) {
      size_t metadata_length;
      st = DBWithTTLImpl::ParseMetadata(operand, nullptr, nullptr,
					&metadata_length);
      if (!st.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, merge_in.logger,
            "Error: Could not remove metadata from operand value.");
        return false;
      }
      operands_without_metadata.push_back(operand);
      operands_without_metadata.back().remove_suffix(metadata_length);
    }

    // Apply the user merge operator (store result in *new_value)
    bool good = true;
    MergeOperationOutput user_merge_out(merge_out->new_value,
                                        merge_out->existing_operand);
    if (merge_in.existing_value) {
      Slice existing_value_without_metadata(merge_in.existing_value->data(),
                                      merge_in.existing_value->size() - existing_value_metadata_len);
      good = user_merge_op_->FullMergeV2(
          MergeOperationInput(merge_in.key, &existing_value_without_metadata,
                              operands_without_metadata, merge_in.logger),
          &user_merge_out);
    } else {
      good = user_merge_op_->FullMergeV2(
          MergeOperationInput(merge_in.key, nullptr, operands_without_metadata,
                              merge_in.logger),
          &user_merge_out);
    }

    // Return false if the user merge operator returned false
    if (!good) {
      return false;
    }

    if (merge_out->existing_operand.data()) {
      merge_out->new_value.assign(merge_out->existing_operand.data(),
                                  merge_out->existing_operand.size());
      merge_out->existing_operand = Slice(nullptr, 0);
    }

    if (merge_in.existing_value) {
      st = DBWithTTLImpl::ParseMetadata(*merge_in.existing_value,
				       &existing_is_expiration,
				       &existing_epoch_time, nullptr);
      if (!st.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, merge_in.logger,
            "Error: Invalid metadata on existing value.");
        return false;
      }

      // Set the existing_epoch_time to write a fresh timestamp when we append
      // metadata below.
      if (!existing_is_expiration) {
        existing_epoch_time = -1;
      }
    }

    // Re-append the existing expiration or append a new timestamp for TTL use.
    st = DBWithTTLImpl::AppendMetadata(merge_out->new_value, existing_epoch_time, env_);
    if (!st.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, merge_in.logger,
            "Error: Could not append updated metadata.");
        return false;
    }
    return true;
  }

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value, Logger* logger) const
      override {
    Status st;
    std::deque<Slice> operands_without_metadata;
    bool existing_is_expiration;
    int32_t existing_epoch_time;

    for (const auto& operand : operand_list) {
      size_t metadata_length;
      // We will retain the latest expiration_time to reapply it.
      st = DBWithTTLImpl::ParseMetadata(operand, &existing_is_expiration,
                                        &existing_epoch_time, &metadata_length);
      if (!st.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, logger,
            "Error: Could not remove metadata from operand value.");
        return false;
      }
      operands_without_metadata.push_back(Slice(operand.data(), operand.size() - metadata_length));
    }

    // Apply the user partial-merge operator (store result in *new_value)
    assert(new_value);
    if (!user_merge_op_->PartialMergeMulti(key, operands_without_metadata, new_value,
                                           logger)) {
      return false;
    }

    // If we're not working with an explicit expiration, bump the timestamp we
    // use for TTL by setting the the epoch time to -1.
    if (!existing_is_expiration) {
      existing_epoch_time = -1;
    }

    // Re-append the existing expiration or append a new timestamp for TTL use.
    st = DBWithTTLImpl::AppendMetadata(*new_value, existing_epoch_time, env_);
    if (!st.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, logger,
            "Error: Could not append updated metadata.");
        return false;
    }

    return true;
  }

  virtual const char* Name() const override { return "Merge By TTL"; }

 private:
  std::shared_ptr<MergeOperator> user_merge_op_;
  Env* env_;
};
}
#endif  // ROCKSDB_LITE
