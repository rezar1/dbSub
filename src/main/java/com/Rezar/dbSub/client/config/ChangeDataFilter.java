package com.Rezar.dbSub.client.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.Rezar.dbSub.utils.GU;
import com.Rezar.logmonitor.jsonLogModule.jsonLogSelectParser.JsonLogQuerySqlParser;
import com.Rezar.logmonitor.jsonLogModule.logFileAnalyzer.whereCond.OptExecute;
import com.Rezar.logmonitor.jsonLogModule.logFileAnalyzer.whereCond.WhereCondition;
import com.google.common.collect.Maps;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月22日 下午7:27:50
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
@Slf4j
public class ChangeDataFilter {

	private String filterSql;
	private WhereCondition whereCondition;

	public static void main(String[] args) {
		ChangeDataFilter changeDataFilter = new ChangeDataFilter(" where (old.status = 1) and new.status = 2");
		Map<String, Object> mixedDatas = new HashMap<>();
		mixedDatas.put("old_status", 1);
		mixedDatas.put("new_status", 2);
		boolean canConsume = changeDataFilter.canConsume(mixedDatas);
		log.info("can consume:{}", canConsume);
	}

	public ChangeDataFilter(String filterSql) {
		if (GU.isNullOrEmpty(filterSql)) {
			return;
		}
		filterSql = transFilterSql(filterSql);
		this.filterSql = "select * from table " + filterSql;
		initWhereFilter();
	}

	/**
	 * 
	 */
	private void initWhereFilter() {
		this.whereCondition = JsonLogQuerySqlParser.createQueryExecutor(this.filterSql).getWhereCondition();
	}

	/**
	 * 
	 * @param mixedDatas
	 *            { key="old_status"-value=1 , key="new_status"=2 ,.... }
	 * @return
	 */
	public boolean canConsume(Map<String, Object> mixedDatas) {
		Map<OptExecute, Boolean> optResult = Maps.newHashMap();
		for (Map.Entry<String, Object> entry : mixedDatas.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			List<OptExecute> findOptExecutes = this.whereCondition.findOptExecutes(key);
			if (GU.notNullAndEmpty(findOptExecutes)) {
				for (OptExecute optExecute : findOptExecutes) {
					boolean optSuccess = optExecute.OptSuccess(value);
					optResult.put(optExecute, optSuccess);
				}
			}
		}
		Map<String, Boolean> optLogResult = Maps.newHashMap();
		for (Map.Entry<OptExecute, Boolean> entry : optResult.entrySet()) {
			optLogResult.put(entry.getKey().toString(), entry.getValue());
		}
		return this.whereCondition.checkWhereIsSuccess(optResult);
	}

	/**
	 * @param filterSql
	 *            : where (old.status = 1) and new.status = 2
	 * @return filterSql : where old_status = 1 and new_status = 2;
	 */
	private String transFilterSql(String filterSql) {
		Pattern pattern = Pattern.compile("(\\(|\\s+)(old|new)\\.", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(filterSql);
		StringBuffer sb = new StringBuffer();
		int index = 0;
		while (matcher.find()) {
			String group1 = matcher.group(1);
			String group2 = matcher.group(2);
			sb.append(filterSql.substring(index, matcher.start())).append(group1).append(group2).append("_");
			index = matcher.end();
		}
		if (index < filterSql.length()) {
			sb.append(filterSql.substring(index, filterSql.length()));
		}
		return sb.toString();
	}

}
