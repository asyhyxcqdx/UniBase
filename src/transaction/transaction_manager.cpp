#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // 1. 判断传入事务参数是否为空指针
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }
    // 2. 初始化事务元数据
    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_++);
    // 3. 把开始事务加入到全局事务表中
    {
        std::lock_guard<std::mutex> lock(latch_);
        txn_map[txn->get_transaction_id()] = txn;
    }
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    // 1. 如果存在未提交的写操作，提交所有的写操作（当前实验未实现日志持久化，直接清空写集）
    for (auto write_record : *txn->get_write_set()) {
        delete write_record;
    }
    txn->get_write_set()->clear();
    // 2. 释放所有锁
    if (lock_manager_ != nullptr) {
        for (const auto &lock_data_id : *txn->get_lock_set()) {
            lock_manager_->unlock(txn, lock_data_id);
        }
    }
    // 3. 清空事务相关资源
    txn->get_lock_set()->clear();
    // 4. 把事务日志刷入磁盘中
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
    // 5. 更新事务状态并移出全局表
    txn->set_state(TransactionState::COMMITTED);
    std::lock_guard<std::mutex> lock(latch_);
    txn_map.erase(txn->get_transaction_id());

}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) {
        return;
    }
    // 1. 回滚所有写操作（逆序撤销）
    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        auto write_record = write_set->back();
        write_set->pop_back();
        if (write_record != nullptr) {
            auto tab_name = write_record->GetTableName();
            auto rid = write_record->GetRid();
            auto wtype = write_record->GetWriteType();
            auto fh_it = sm_manager_->fhs_.find(tab_name);
            if (fh_it != sm_manager_->fhs_.end()) {
                auto *fh = fh_it->second.get();
                if (wtype == WType::INSERT_TUPLE) {
                    fh->delete_record(rid, nullptr);
                } else if (wtype == WType::DELETE_TUPLE) {
                    fh->insert_record(rid, write_record->GetRecord().data);
                } else if (wtype == WType::UPDATE_TUPLE) {
                    fh->update_record(rid, write_record->GetRecord().data, nullptr);
                }
            }
            delete write_record;
        }
    }
    // 2. 释放所有锁
    if (lock_manager_ != nullptr) {
        for (const auto &lock_data_id : *txn->get_lock_set()) {
            lock_manager_->unlock(txn, lock_data_id);
        }
    }
    // 3. 清空事务相关资源
    txn->get_lock_set()->clear();
    // 4. 把事务日志刷入磁盘中
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
    // 5. 更新事务状态并移出全局表
    txn->set_state(TransactionState::ABORTED);
    std::lock_guard<std::mutex> lock(latch_);
    txn_map.erase(txn->get_transaction_id());
}
