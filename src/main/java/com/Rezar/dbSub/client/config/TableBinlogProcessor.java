package com.Rezar.dbSub.client.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.Rezar.dbSub.base.dbInfo.TableMark;
import com.Rezar.dbSub.base.enums.ChangeType;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.client.interfaces.ChangeDataHandler;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.MyMapEntry;
import com.Rezar.dbSub.utils.StrNameConvert;
import com.lmax.disruptor.EventHandler;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月22日 下午7:26:24
 * @Desc 些年若许,不负芳华.
 * 
 *       用于处理
 */
@Data
@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TableBinlogProcessor implements EventHandler<SyncEvent> {

	private TableMark tableMark;
	// 将SyncEvent里面(oldData,newData)map转换为业务处理器接收的实体对象
	private EntityParser entityParer;
	// binlog业务处理器
	private ChangeDataHandler changeDataHandler;
	// sql格式的数据过滤,如当前业务处理器只处理
	// （old.status = 1 and old.create_time > '2020-05-23 15:42:24'）
	// 的变更数据
	private ChangeDataFilter changeDataFilter;
	// 关注的binlog事件类型,如只处理更新事件
	private List<ChangeType> acceptChangeTypes;

	// 复用Map,用于给过滤数据的SQL执行提供数据源
	private Map<String, Object> cacheMap = new HashMap<>();

	/**
	 * @param entityParser
	 * @param changeDataFilter
	 * @param handler
	 */
	public TableBinlogProcessor(EntityParser entityParser, List<ChangeType> acceptChangeTypes,
			ChangeDataFilter changeDataFilter, ChangeDataHandler handler) {
		this.acceptChangeTypes = acceptChangeTypes;
		this.entityParer = entityParser;
		this.changeDataFilter = changeDataFilter;
		this.changeDataHandler = handler;
	}

	@Override
	public void onEvent(SyncEvent event, long sequence, boolean endOfBatch) throws Exception {
		ChangeType changeType = ChangeType.valueOf(event.eventType);
		if (!this.getAcceptChangeTypes().contains(changeType)) {
			return;
		}
		this.parseAndCheck(event).ifPresent(data -> {
			try {
				this.getChangeDataHandler().onEvent(data.getLeft(), data.getRight(), event.getSeqId(),
						event.getTimestamp(), changeType);
			} catch (Exception e) {
				log.error("ChangeDataHandler encount a exception:{}", e);
			}
		});
	}

	/**
	 * 
	 * @return
	 */
	private Optional<Pair<Object, Object>> parseAndCheck(SyncEvent event) {
		Map<String, Object> changeDataOfOld = changeData(event.getOldDatas());
		Map<String, Object> changeDataOfNew = changeData(event.getNewDatas());
		if (this.changeDataFilter != null) {
			try {
				if (changeDataOfOld != null) {
					this.cacheMap.putAll(
							changeDataOfOld.entrySet().stream().filter(entry -> Objects.nonNull(entry.getValue()))
									.collect(Collectors.toMap(entry -> "old_" + entry.getKey(), Entry::getValue)));
				}
				if (changeDataOfNew != null) {
					this.cacheMap.putAll(
							changeDataOfNew.entrySet().stream().filter(entry -> Objects.nonNull(entry.getValue()))
									.collect(Collectors.toMap(entry -> "new_" + entry.getKey(), Entry::getValue)));
				}
				if (!this.changeDataFilter.canConsume(cacheMap)) {
					return Optional.empty();
				}
			} finally {
				this.cacheMap.clear();
			}
		}
		return Optional.of(
				Pair.of(this.entityParer.parseToObj(changeDataOfOld), this.entityParer.parseToObj(changeDataOfNew)));
	}

	private Map<String, Object> changeData(Map<String, Object> dataMap) {
		if (GU.isNullOrEmpty(dataMap)) {
			return null;
		}
		return dataMap.entrySet().stream()
				.map(entry -> new MyMapEntry<String, Object>(StrNameConvert.underlineToHump(entry.getKey()),
						entry.getValue()))
				.filter(entry -> entry.getValue() != null).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
	}

	public static List<TableBinlogProcessor> fromSubTableInfo(SubTableInfo subDbInfo) {
		EntityParser entityParser = null;
		try {
			if (subDbInfo.getRealEntityClass() != null) {
				entityParser = new EntityParser(subDbInfo.getRealEntityClass());
			} else {
				entityParser = new EntityParser(subDbInfo.getEntityClass());
			}
		} catch (ClassNotFoundException e) {
			log.error(String.format("invalid class:%s ,error:{}", subDbInfo.getEntityClass(), e));
		}
		List<SingleSubHandler> subItems = subDbInfo.getHandlers();
		List<TableBinlogProcessor> retList = new ArrayList<TableBinlogProcessor>(subItems.size());
		for (SingleSubHandler singleSubHandler : subItems) {
			ChangeDataFilter changeDataFilter = null;
			if (GU.notNullAndEmpty(singleSubHandler.getFilter())) {
				changeDataFilter = new ChangeDataFilter(singleSubHandler.getFilter());
			}
			ChangeDataHandler handler = singleSubHandler.getChangeDataHandler();
			if (handler == null) {
				log.info("error init eventHandler with config:{}", subDbInfo);
			}
			retList.add(new TableBinlogProcessor(entityParser, Arrays.asList(singleSubHandler.getAcceptChangeTypes()),
					changeDataFilter, handler));
		}
		return retList;
	}

}
