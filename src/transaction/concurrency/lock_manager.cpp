#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::SHARED);
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {

    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::SHARED);
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_SHARED);
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_EXCLUSIVE);
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = lock_table_.find(lock_data_id);
    if (it == lock_table_.end()) {
        return false;
    }
    auto &queue = it->second;
    for (auto iter = queue.request_queue_.begin(); iter != queue.request_queue_.end(); ++iter) {
        if (iter->txn_id_ == txn->get_transaction_id()) {
            queue.request_queue_.erase(iter);
            if (txn != nullptr) {
                txn->get_lock_set()->erase(lock_data_id);
                if (txn->get_state() == TransactionState::GROWING) {
                    txn->set_state(TransactionState::SHRINKING);
                }
            }
            if (queue.request_queue_.empty()) {
                lock_table_.erase(it);
            } else {
                update_group_lock_mode(queue);
            }
            queue.cv_.notify_all();
            return true;
        }
    }
    return false;
}

bool LockManager::lock(Transaction* txn, const LockDataId& lock_data_id, LockMode lock_mode) {
    if (txn == nullptr) {
        return false;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    std::unique_lock<std::mutex> lock_guard(latch_);
    auto &queue = lock_table_[lock_data_id];

    // 检查是否已经持有锁或正在等待同一锁
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ != txn->get_transaction_id()) {
            continue;
        }
        // 已持有同类型锁
        if (it->granted_ && it->lock_mode_ == lock_mode) {
            return true;
        }
        // 锁升级
        bool conflict = false;
        for (auto &req : queue.request_queue_) {
            if (req.txn_id_ == txn->get_transaction_id()) {
                continue;
            }
            if (req.granted_ && !is_compatible(lock_mode, req.lock_mode_)) {
                conflict = true;
                break;
            }
        }
        if (conflict) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }
        it->lock_mode_ = lock_mode;
        it->granted_ = true;
        update_group_lock_mode(queue);
        txn->get_lock_set()->insert(lock_data_id);
        return true;
    }

    // 检查与当前已授权锁是否兼容
    for (auto &req : queue.request_queue_) {
        if (req.granted_ && !is_compatible(lock_mode, req.lock_mode_)) {
            return false;
        }
    }

    // 加入队列并授权
    queue.request_queue_.emplace_back(txn->get_transaction_id(), lock_mode);
    queue.request_queue_.back().granted_ = true;
    update_group_lock_mode(queue);
    txn->get_lock_set()->insert(lock_data_id);
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    return true;
}

LockManager::GroupLockMode LockManager::to_group_lock_mode(LockMode lock_mode) {
    switch (lock_mode) {
        case LockMode::SHARED:
            return GroupLockMode::S;
        case LockMode::EXLUCSIVE:
            return GroupLockMode::X;
        case LockMode::INTENTION_SHARED:
            return GroupLockMode::IS;
        case LockMode::INTENTION_EXCLUSIVE:
            return GroupLockMode::IX;
        case LockMode::S_IX:
            return GroupLockMode::SIX;
        default:
            return GroupLockMode::NON_LOCK;
    }
}

bool LockManager::is_compatible(LockMode lhs, LockMode rhs) {
    static const bool compat[6][6] = {
        /*            NON   IS     IX     S      X      SIX */
        /*NON*/ {true, true,  true,  true,  true,  true},
        /*IS */ {true, true,  true,  true,  false, true},
        /*IX */ {true, true,  true,  false, false, false},
        /*S  */ {true, true,  false, true,  false, false},
        /*X  */ {true, false, false, false, false, false},
        /*SIX*/ {true, true,  false, false, false, false}};
    auto l = static_cast<int>(to_group_lock_mode(lhs));
    auto r = static_cast<int>(to_group_lock_mode(rhs));
    return compat[l][r] && compat[r][l];
}

void LockManager::update_group_lock_mode(LockRequestQueue& queue) {
    GroupLockMode mode = GroupLockMode::NON_LOCK;
    auto priority = [](GroupLockMode m) {
        switch (m) {
            case GroupLockMode::NON_LOCK:
                return 0;
            case GroupLockMode::IS:
                return 1;
            case GroupLockMode::IX:
                return 2;
            case GroupLockMode::S:
                return 3;
            case GroupLockMode::SIX:
                return 4;
            case GroupLockMode::X:
                return 5;
            default:
                return 0;
        }
    };
    for (auto &req : queue.request_queue_) {
        if (!req.granted_) {
            continue;
        }
        auto gm = to_group_lock_mode(req.lock_mode_);
        if (priority(gm) > priority(mode)) {
            mode = gm;
        }
    }
    queue.group_lock_mode_ = mode;
}
