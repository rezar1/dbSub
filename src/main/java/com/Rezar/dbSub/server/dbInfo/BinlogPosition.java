package com.Rezar.dbSub.server.dbInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.github.shyiko.mysql.binlog.GtidSet;

public class BinlogPosition {

	private static final String FILE_COLUMN = "File";
	private static final String POSITION_COLUMN = "Position";
	private static final String GTID_COLUMN = "Executed_Gtid_Set";

	private final String gtidSetStr;
	private final String gtid;
	private final long offset;
	private final String file;

	public BinlogPosition(String gtidSetStr, String gtid, long l, String file) {
		this.gtidSetStr = gtidSetStr;
		this.gtid = gtid;
		this.offset = l;
		this.file = file;
	}

	public BinlogPosition(long l, String file) {
		this(null, null, l, file);
	}

	public byte[] toBytes() {
		return String.format("%s/%s", this.file, this.offset).getBytes();
	}

	/**
	 * @param bs
	 * @return
	 */
	public static BinlogPosition fromBytes(byte[] bs) {
		if (bs == null || bs.length == 0) {
			return null;
		}
		String fileOffsetSingle = new String(bs);
		String[] split = fileOffsetSingle.split("/");
		return new BinlogPosition(Long.parseLong(split[1]), split[1]);
	}

	public static BinlogPosition capture(Connection c, boolean gtidMode) throws SQLException {
		ResultSet rs;
		rs = c.createStatement().executeQuery("SHOW MASTER STATUS");
		rs.next();
		long l = rs.getInt(POSITION_COLUMN);
		String file = rs.getString(FILE_COLUMN);
		String gtidSetStr = null;
		if (gtidMode) {
			gtidSetStr = rs.getString(GTID_COLUMN);
		}
		return new BinlogPosition(gtidSetStr, null, l, file);
	}

	public static BinlogPosition at(BinlogPosition position) {
		return new BinlogPosition(position.gtidSetStr, position.gtid, position.offset, position.file);
	}

	public static BinlogPosition at(String gtidSetStr, long offset, String file) {
		return new BinlogPosition(gtidSetStr, null, offset, file);
	}

	public static BinlogPosition at(long offset, String file) {
		return new BinlogPosition(null, null, offset, file);
	}

	public long getOffset() {
		return offset;
	}

	public String getFile() {
		return file;
	}

	public String getGtid() {
		return gtid;
	}

	public String getGtidSetStr() {
		return gtidSetStr;
	}

	public GtidSet getGtidSet() {
		return new GtidSet(gtidSetStr);
	}

	@Override
	public String toString() {
		return "BinlogPosition[" + (gtidSetStr == null ? file + ":" + offset : gtidSetStr) + "]";
	}

	public String fullPosition() {
		String pos = file + ":" + offset;
		if (gtidSetStr != null)
			pos += "[" + gtidSetStr + "]";
		return pos;
	}

	public boolean newerThan(BinlogPosition other) {
		if (other == null)
			return true;

		if (gtidSetStr != null) {
			return !getGtidSet().isContainedWithin(other.getGtidSet());
		}

		int cmp = this.file.compareTo(other.file);
		if (cmp > 0) {
			return true;
		} else if (cmp == 0) {
			return this.offset > other.offset;
		} else {
			return false;
		}
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof BinlogPosition))
			return false;
		BinlogPosition otherPosition = (BinlogPosition) other;
		return this.file.equals(otherPosition.file) && this.offset == otherPosition.offset && (gtidSetStr == null
				? otherPosition.gtidSetStr == null : gtidSetStr.equals(otherPosition.gtidSetStr));
	}

	@Override
	public int hashCode() {
		if (gtidSetStr != null) {
			return gtidSetStr.hashCode();
		} else {
			return Long.valueOf(offset).hashCode();
		}
	}

}
