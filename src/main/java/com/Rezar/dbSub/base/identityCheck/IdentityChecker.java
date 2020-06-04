package com.Rezar.dbSub.base.identityCheck;

import org.apache.commons.lang3.tuple.Pair;

public interface IdentityChecker {

	public Pair<Boolean, String> valid(String clientHost, String username, String password);

}
