#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // 1. 获取指定记录所在的page handle
    auto page_handle = fetch_page_handle(rid.page_no);
    // 2. 若对应slot不存在记录则抛出异常
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page({fd_, rid.page_no}, false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    // 3. 拷贝记录内容返回
    auto rec = std::make_unique<RmRecord>(file_hdr_.record_size, page_handle.get_slot(rid.slot_no));
    buffer_pool_manager_->unpin_page({fd_, rid.page_no}, false);
    return rec;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 1. 获取当前未满的page handle
    auto page_handle = create_page_handle();
    auto page_no = page_handle.page->get_page_id().page_no;
    // 2. 在page handle中找到空闲slot位置
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    if (slot_no >= file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page({fd_, page_no}, false);
        throw InternalError("No free slot found when inserting record");
    }
    // 3. 将buf复制到空闲slot位置
    memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, slot_no);
    // 4. 更新page_handle.page_hdr中的数据结构
    page_handle.page_hdr->num_records++;
    // 页面已满则更新空闲链表头
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    }
    buffer_pool_manager_->unpin_page({fd_, page_no}, true);
    return Rid{page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    auto page_handle = fetch_page_handle(rid.page_no);
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page({fd_, rid.page_no}, false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;
    // 如果页面写满，需要将其从空闲链表中移除
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        int target = rid.page_no;
        if (file_hdr_.first_free_page_no == target) {
            file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        } else {
            int prev = file_hdr_.first_free_page_no;
            while (prev != RM_NO_PAGE) {
                auto prev_handle = fetch_page_handle(prev);
                if (prev_handle.page_hdr->next_free_page_no == target) {
                    prev_handle.page_hdr->next_free_page_no = page_handle.page_hdr->next_free_page_no;
                    buffer_pool_manager_->unpin_page({fd_, prev}, true);
                    break;
                }
                int next = prev_handle.page_hdr->next_free_page_no;
                buffer_pool_manager_->unpin_page({fd_, prev}, false);
                prev = next;
            }
        }
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    }
    buffer_pool_manager_->unpin_page({fd_, rid.page_no}, true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 1. 获取指定记录所在的page handle
    auto page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page({fd_, rid.page_no}, false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    bool was_full = page_handle.page_hdr->num_records == file_hdr_.num_records_per_page;
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;
    // 2. 如果原本已满，删除后需要将该页加入空闲链表
    if (was_full) {
        release_page_handle(page_handle);
    }
    buffer_pool_manager_->unpin_page({fd_, rid.page_no}, true);
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 1. 获取指定记录所在的page handle
    auto page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page({fd_, rid.page_no}, false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    // 2. 更新记录
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    buffer_pool_manager_->unpin_page({fd_, rid.page_no}, true);

}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if (page_no < RM_FIRST_RECORD_PAGE || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    }
    PageId pid{fd_, page_no};
    auto page = buffer_pool_manager_->fetch_page(pid);
    if (page == nullptr) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    }
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 1.使用缓冲池来创建一个新page
    PageId new_pid{fd_, INVALID_PAGE_ID};
    auto page = buffer_pool_manager_->new_page(&new_pid);
    if (page == nullptr) {
        throw InternalError("Failed to allocate new page for record file");
    }
    // 2.更新page handle中的相关信息
    RmPageHandle page_handle(&file_hdr_, page);
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);
    page_handle.page_hdr->num_records = 0;
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    // 3.更新file_hdr_
    file_hdr_.first_free_page_no = new_pid.page_no;
    file_hdr_.num_pages++;
    buffer_pool_manager_->unpin_page(new_pid, true);
    // 返回新创建的page handle（重新fetch，保证page处于pinned状态供上层使用）
    return fetch_page_handle(new_pid.page_no);
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // 1. 判断file_hdr_中是否还有空闲页
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        // 1.1 没有空闲页：创建新页
        return create_new_page_handle();
    }
    // 1.2 有空闲页：直接获取第一个空闲页
    return fetch_page_handle(file_hdr_.first_free_page_no);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // 当page从已满变成未满，将其接入空闲链表头
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}
