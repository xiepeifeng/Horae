package com.tencent.isd.lhotse.dao;

import java.util.ArrayList;

/**
 * @author: cpwang
 * @date: 2012-11-19
 */
public class StatRange {
	private int id;
	private float floor;
	private float ceil;
	private String description;

	private final static StatRange UNKNOWN_RANGE = new StatRange(9, 0, 0, "UNKNOWN");
	private final static ArrayList<StatRange> durationRanges = new ArrayList<StatRange>();
	private final static ArrayList<StatRange> flutterRanges = new ArrayList<StatRange>();

	static {
		durationRanges.add(new StatRange(1, 0, 10L, "0~10分钟"));
		durationRanges.add(new StatRange(2, 10L, 30L, "10分钟~30分钟"));
		durationRanges.add(new StatRange(3, 30L, 60L, "30分钟~60分钟"));
		durationRanges.add(new StatRange(4, 60L, Float.MAX_VALUE, "60分钟以上"));

		flutterRanges.add(new StatRange(-4, -Float.MAX_VALUE, -100, "-100%以下"));
		flutterRanges.add(new StatRange(-3, -100, -50, "-100%~-50%"));
		flutterRanges.add(new StatRange(-2, -50, -20, "-50%~-20%"));
		flutterRanges.add(new StatRange(-1, -20, 0, "-20%~0"));
		flutterRanges.add(new StatRange(1, 0, 20, "0~20%"));
		flutterRanges.add(new StatRange(2, 20, 50, "20%~50%"));
		flutterRanges.add(new StatRange(3, 50, 100, "50%~100%"));
		flutterRanges.add(new StatRange(4, 100, Float.MAX_VALUE, "100%以上"));
	}

	public StatRange(int id, float floor, float ceil, String desc) {
		this.id = id;
		this.floor = floor;
		this.ceil = ceil;
		this.description = desc;
	}

	public int getId() {
		return id;
	}

	public float getFloor() {
		return floor;
	}

	public float getCeil() {
		return ceil;
	}

	public String getDescription() {
		return description;
	}

	public static StatRange getDurationRange(float d) {
		for (StatRange dr : durationRanges) {
			if ((dr.getFloor() <= d) && (dr.getCeil() > d)) {
				return dr;
			}
		}
		return UNKNOWN_RANGE;
	}

	public static StatRange getFlutterRange(float f) {
		for (StatRange fr : flutterRanges) {
			if ((fr.getFloor() <= f) && (fr.getCeil() > f)) {
				return fr;
			}
		}
		return UNKNOWN_RANGE;
	}

}
