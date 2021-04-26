package com.hl.ir.utilities.dynamicorm;

import java.sql.Connection;

import com.hl.ir.utilities.dynamicorm.impl.HlDbUtilInteractor;
import com.hl.ir.utilities.dynamicorm.model.DbInteractorModel;

public abstract class DatabaseInteractor {

	protected Connection connection; 
	
	public abstract Object select(DbInteractorModel dbInteractorModel);
	public abstract int delete();
	public abstract Object insert(DbInteractorModel dbOperationModel);
	public abstract int update(DbInteractorModel dbOperationModel);
	
	public static DatabaseInteractor getDatabaseInteractor(Connection connection) {
		return new HlDbUtilInteractor(connection);
	}
}
