package com.Rezar.dbSub.testHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import com.Rezar.dbSub.base.EventHandlerAnnot;
import com.Rezar.dbSub.base.enums.ChangeType;
import com.Rezar.dbSub.client.interfaces.ChangeDataHandler;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月29日 下午3:43:17
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
@Service
@EventHandlerAnnot(dbInstance = "dsp-ad", db = "dsp", tableName = "idea_ext_audit", filter = "")
public class IdeaExtAuditChangeDataHandler implements ChangeDataHandler<TestIdeaExtAudit> {

	@Getter
	private String curOffset;

	private volatile long start;
	private volatile long end;

	@Override
	public boolean onEvent(TestIdeaExtAudit oldData, TestIdeaExtAudit newData, String eventMsgId, long timestamp,
			ChangeType changeType) {
		long cur = System.nanoTime();
		if (start == 0) {
			start = cur;
			CompletableFuture.runAsync(() -> {
				long preUseTime = 0;
				while (true) {
					try {
						TimeUnit.SECONDS.sleep(8);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					long useTime = (end - start);
					if (preUseTime == useTime) {
						break;
					} else {
						preUseTime = useTime;
					}
				}
				log.info("use time:{}", preUseTime);
				this.start = 0;
			});
		}
		this.end = cur;
//		log.info("eventMsgId:{} changeType:{} oldData:{} newData:{}", eventMsgId, changeType,
//				JacksonUtil.obj2Str(oldData), JacksonUtil.obj2Str(newData));
		if (oldData.getIdeaAuditId() == null) {
			log.info("miss null value");
		}
		curOffset = eventMsgId;
		return false;
	}

}
