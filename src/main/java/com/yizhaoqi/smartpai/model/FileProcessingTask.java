package com.yizhaoqi.smartpai.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 文件处理任务类，用于Kafka消息传递
 * 把「需要处理的文件信息」打包成一个消息对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileProcessingTask {
    /**
        两种任务类型：UPLOAD_PROCESS用户上传文件后合并 和 REINDEX管理员手动触发重建索引
    */
    public static final String TASK_TYPE_UPLOAD_PROCESS = "UPLOAD_PROCESS";
    public static final String TASK_TYPE_REINDEX = "REINDEX";

    private String fileMd5; // 文件的 MD5 校验值
    private String filePath; // 文件存储路径
    private String fileName; // 文件名
    private String userId;   // 上传用户ID
    private String orgTag;   // 文件所属组织标签
    private boolean isPublic; // 文件是否公开
    private String taskType; // 任务类型
    private String requesterId; // 发起重试的用户
    
    /**
     * 向后兼容的构造函数
     */
    public FileProcessingTask(String fileMd5, String filePath, String fileName) {
        this.fileMd5 = fileMd5;
        this.filePath = filePath;
        this.fileName = fileName;
        this.userId = null;
        this.orgTag = "DEFAULT";
        this.isPublic = false;
        this.taskType = TASK_TYPE_UPLOAD_PROCESS;
        this.requesterId = null;
    }
}
