package vg.civcraft.mc.civmodcore.database;

import java.util.logging.Logger;

public abstract class DataBaseManager {
	protected DataBase db;

	public DataBaseManager(String host, int port, String db, String user,
			String password, Logger logger) {
		this.db = new DataBase(host, port, db, user, password, logger);
		if (!this.db.connect()) {
			logger.severe("Could not connect to database");
			return;
		}
		prepareTables();
		loadPreparedStatements();
	}

	public abstract void prepareTables();

	public abstract void loadPreparedStatements();

	public boolean isConnected() {
		if (!db.isConnected()) {
			db.connect();
			if (db.isConnected()) {
				loadPreparedStatements();
			}
		}
		return db.isConnected();
	}
}
