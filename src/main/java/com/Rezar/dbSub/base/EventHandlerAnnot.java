package com.Rezar.dbSub.base;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.Rezar.dbSub.base.enums.ChangeType;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年12月8日 上午10:40:13
 * @Desc 些年若许,不负芳华.
 *
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface EventHandlerAnnot {

	String dbInstance();

	String db();

	String tableName();

	String filter() default "";

	ChangeType[] acceptType() default { ChangeType.INSERT, ChangeType.UPDATE, ChangeType.DELETE };

}
