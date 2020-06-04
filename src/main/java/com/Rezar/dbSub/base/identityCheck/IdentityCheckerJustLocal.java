package com.Rezar.dbSub.base.identityCheck;

import org.apache.commons.lang3.tuple.Pair;

import com.Rezar.dbSub.utils.GU;

public class IdentityCheckerJustLocal implements IdentityChecker {

	@Override
	public Pair<Boolean, String> valid(String clientHost, String username, String password) {
		boolean ok = GU.notNullAndEmpty(username) && GU.notNullAndEmpty(password)
				&& username.trim().contentEquals("Rezar") && password.contentEquals("HelloWorld");
		return Pair.of(ok, ok ? "^_^" : "凭证无效哦");
	}

}
