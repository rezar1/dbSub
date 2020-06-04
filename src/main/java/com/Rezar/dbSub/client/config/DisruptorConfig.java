package com.Rezar.dbSub.client.config;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;

import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.utils.beanUtil.BeanInvokeUtils;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月27日 下午6:51:10
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class DisruptorConfig {

	public static EventFactory<SyncEvent> eventInitFacotry = new EventFactory<SyncEvent>() {
		@Override
		public SyncEvent newInstance() {
			SyncEvent event = SyncEvent.nothing();
			return event;
		}
	};

	public static final UncaughtExceptionHandler eh = new UncaughtExceptionHandler() {
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			e.printStackTrace();
			log.error(String.format("process - %s encount a exception:{}", t.getName()), e);
		}
	};

	// 用于事件处理(EventProcessor)的线程工厂
	public static ThreadFactory threadFactory = new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			String threadName = "业务处理器线程:";
			if (r instanceof TableBinlogProcessor) {
				threadName += ((TableBinlogProcessor) r).getTableMark().getStrMarkId();
			}
			Thread thread = new Thread(r, threadName);
			Thread.setDefaultUncaughtExceptionHandler(eh);
			return thread;
		}
	};

	public static EventTranslatorOneArg<SyncEvent, SyncEvent> TRANS_INSTANCE = new EventTranslatorOneArg<SyncEvent, SyncEvent>() {
		@Override
		public void translateTo(SyncEvent event, long sequence, SyncEvent arg0) {
			BeanInvokeUtils.copyBean(arg0, event);
		}
	};

}
