#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化rid，指向第一个存放了记录的位置
    rid_ = Rid{RM_FIRST_RECORD_PAGE, -1};
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    if (file_handle_->file_hdr_.num_pages <= RM_FIRST_RECORD_PAGE) {
        rid_.page_no = RM_NO_PAGE;
        rid_.slot_no = RM_NO_PAGE;
        return;
    }
    int start_page = rid_.page_no;
    int start_slot = rid_.slot_no;
    for (int page_no = start_page; page_no < file_handle_->file_hdr_.num_pages; page_no++) {
        auto page_handle = file_handle_->fetch_page_handle(page_no);
        int begin_slot = (page_no == start_page ? start_slot : -1);
        int slot_no = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, begin_slot);
        file_handle_->buffer_pool_manager_->unpin_page({file_handle_->fd_, page_no}, false);
        if (slot_no < file_handle_->file_hdr_.num_records_per_page) {
            rid_.page_no = page_no;
            rid_.slot_no = slot_no;
            return;
        }
    }
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = RM_NO_PAGE;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}
