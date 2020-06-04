package com.Rezar.dbSub.server.dbInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.Rezar.dbSub.utils.SeriAndDeser;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月25日 下午3:11:38
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class TableColumnList {

	private final List<ColumnDef> columns = new ArrayList<>();
	private Set<String> columnNames = new HashSet<>();

	public void addColumnDef(ColumnDef columnDef) {
		this.columns.add(columnDef);
		this.columnNames.add(columnDef.getName());
	}

	@Data
	public static class ColumnDef {
		private String name;
		private byte type;
		private int pos;
		private String charset;

		public ColumnDef(String name, String charset, String type) {
			this.name = name;
			this.charset = charset;
			this.type = initType(type);
		}

		public Byte initType(String type) {
			switch (type) {
			case "tinyint":
			case "smallint":
			case "mediumint":
			case "int":
				return SeriAndDeser.INTEGER;
			case "bigint":
				return SeriAndDeser.LONG;
			case "tinytext":
			case "text":
			case "mediumtext":
			case "longtext":
			case "varchar":
			case "char":
				return SeriAndDeser.STRING;
			// case "tinyblob":
			// case "blob":
			// case "mediumblob":
			// case "longblob":
			// case "binary":
			// case "varbinary":
			// return new StringColumnDef(name, type, pos, "binary");
			// case "geometry":
			// case "geometrycollection":
			// case "linestring":
			// case "multilinestring":
			// case "multipoint":
			// case "multipolygon":
			// case "polygon":
			// case "point":
			// return new GeometryColumnDef(name, type, pos);
			case "float":
				return SeriAndDeser.FLOAT;
			case "double":
				return SeriAndDeser.DOUBLE;
			case "decimal":
				return SeriAndDeser.DECIMAL;
			case "date":
				return SeriAndDeser.DATE;
			case "datetime":
				return SeriAndDeser.DATETIME;
			case "timestamp":
				return SeriAndDeser.TIMESTAMP;
			case "time":
				return SeriAndDeser.TIME;
			// case "year":
			// return ColumnType.YEAR;
			// case "enum":
			// return new EnumColumnDef(name, type, pos, enumValues);
			// case "set":
			// return new SetColumnDef(name, type, pos, enumValues);
			// case "bit":
			// return new BitColumnDef(name, type, pos);
			// case "json":
			// return new JsonColumnDef(name, type, pos);
			default:
				throw new IllegalArgumentException("unsupported column type " + type);
			}
		}

	}

}
