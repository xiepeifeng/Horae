package com.tencent.isd.lhotse.runner.api.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {
	private static final String DAY_EXPR_PATH = "${YYYYMMDD}";
	private static final String HOUR_EXPR_PATH = "${YYYYMMDDHH}";
	private static final String MONTH_EXPR_PATH = "${YYYYMM}";
	private static final String DAY_EXPR_PARTITION = "YYYYMMDD";
	private static final String HOUR_EXPR_PARTITION = "YYYYMMDDHH";
	private static final String MONTH_EXPR_PARTITION = "YYYYMM";
	
	/* Replace the date regression string in the path. */
	public static String replaceDateExpr(String originalExpr, Date runDate) {
		if (originalExpr == null) {
			return null;
		}
		
		String year = new SimpleDateFormat("yyyy").format(runDate);
		String month = new SimpleDateFormat("MM").format(runDate);
		String day = new SimpleDateFormat("dd").format(runDate);
		String hour = new SimpleDateFormat("HH").format(runDate);
		
		/* Replace those date expression in the path. */
		originalExpr = originalExpr.replace(HOUR_EXPR_PATH, year + month + day + hour);
		originalExpr = originalExpr.replace(DAY_EXPR_PATH, year + month + day);
		originalExpr = originalExpr.replace(MONTH_EXPR_PATH, year + month);
		
		/* Replace those data expression in the partition expression. */
		originalExpr = originalExpr.replace(HOUR_EXPR_PARTITION, year + month + day + hour);
		originalExpr = originalExpr.replace(DAY_EXPR_PARTITION, year + month + day);
		originalExpr = originalExpr.replace(MONTH_EXPR_PARTITION, year + month);
				
		return originalExpr;
	}
	
	public static PartitionType getTDWTablePartitionType(String partitionType) {
		if (partitionType.contains(HOUR_EXPR_PARTITION)) {
			return PartitionType.HOUR;
		}
		
		if (partitionType.contains(DAY_EXPR_PARTITION)) {
			return PartitionType.DAY;
		}
		
		if (partitionType.contains(MONTH_EXPR_PARTITION)) {
			return PartitionType.MONTH;
		}
		
		return PartitionType.ERROR;
	}
	
	public static enum PartitionType {
		HOUR("H"),
		DAY("D"),
		MONTH("M"),
		ERROR("E");
		
		private final String type;
		
		private PartitionType(String type) {
			this.type = type;
		}
		
		public String getType() {
			return type;
		}
		
		public static PartitionType getPartitionType(String partitionType) {
			if ("M".equals(partitionType)) {
				return MONTH;				
			} else if ("D".equals(partitionType)) {
				return DAY;
			} else if ("H".equals(partitionType)) {
				return HOUR;
			}			
			
			return ERROR;
		}
		
		public boolean isSameType(PartitionType partitionType) {
			return type.equals(partitionType.getType());
		}
	}
}
