package com.Rezar.dbSub.testHandler;

import java.sql.Timestamp;
import java.util.Date;

import lombok.Data;

@Data
public class TestIdeaExtAudit {
	private Integer ideaAuditId;
	private Integer ideaId;
	private Integer zmAuditStatus;
	private Integer adxId;
	private Integer mediaId;
	private Integer adxAuditStatus;
	private String zmReason;
	private String adxReason;
	private String adxIdeaId;
	private Date createTime;
	private Date lastModifyTime;
	private Date uploadTime;
	private String adxIdeaUpId;
	private String extData;
	private Timestamp zmAuditModifyTime;
	private Timestamp adxAuditModifyTime;
	private Integer isDelete;
	private Integer assetId;
	private Date endTime;
}