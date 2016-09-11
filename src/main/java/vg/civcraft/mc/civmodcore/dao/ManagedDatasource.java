package vg.civcraft.mc.civmodcore.dao;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import vg.civcraft.mc.civmodcore.ACivMod;

/**
 * Plugins should replace their custom Database handlers with an instance of ManagedDatasource.
 * <br /><br />
 * See the {@link #ManagedDatasource(ACivMod, String, String, String, int, String, int, long, long, long)} constructor for details on how to use the ManagedDatasource.
 * <br /><br />
 * To convert existing plugins, do the following:
 * <br /><br />
 * <ol>
 *   <li> Take existing database version code and refactor it.
 *      <ol><li> Any CREATE, UPDATE, ALTER, or similar statements, convert to a List of Strings and pass
 *         them into ManagedDatasource as a Migration using {@link #registerMigration(Integer, boolean, String...)}
 *      <li> Find your prepared statements. Convert the string resources as static final in your plugin's DAO layer.
 *      <li> Remove any "is database alive" check code. It's not needed.
 *      <li> Remove any version management code that remains
 *      <li> DO react to the results of the {@link #upgradeDatabase} call. 
 *         <ol><li>   If false is returned, terminate your plugin.
 *         <li>  If false is returned and your plugin is critical to a host of other plugins, terminate the server.
 *         <li> If an Exception is thrown, I strongly recommend you consider it a "false" return value and react accordingly.
 *      </ol><li> Note: Create a "first" migration at index -1 that ignores errors and copies any "current" migration state data
 *         from the db_version or equivalent table into the <code>managed_plugin_data</code> table. 
 *  </ol><li> Throughout your plugin, ensure that PreparedStatements are "created" new each request and against a 
 *     newly retrieved Connection (using {@link #getConnection()} of this class). Don't worry about PrepareStatements.
 *     The driver will manage caching them efficiently.
 *  <li> Make sure you release Connections using {@link Connection#close()} as soon as you can (when done with them). 
 *     a. Don't hold on to Connections. 
 *     b. Close them. 
 *     c. Use "try-with-resources" where-ever possible so that they are auto-closed.
 *  <li> If you have loops to insert a bunch of similar records, convert it to a batch. Find instructions in {@link #ManagedDatasource}.
 *  <li> If you have special needs like atomic multi-statement, do all your work on a single Connection and return it to 
 *     a clean state when you are done. (turn auto-commit back on, ensure all transactions are committed, etc.)
 *</ol>
 * <br /><br />
 * That should cover most cases. Note that points 2 & 3 are critical. Point 1 is required. Point 4 and 5 are highly recommended.
 * <br /><br />
 * TODO: Make this instantiable via ConfigurationSerializable interface.
 *
 * @author ProgrammerDan
 */
public class ManagedDatasource {

	private static final long MAX_WAIT_FOR_LOCK = 600000l;
	private static final long WAIT_PERIOD = 500l;
	
	private HikariDataSource connections;
	private HikariConfig config;
	private Logger logger;
	private ACivMod plugin;
	
	private static final String CHECK_CREATE_MIGRATIONS_TABLE = 
			"CREATE TABLE IF NOT EXISTS managed_plugin_data ("
					+ "managed_id BIGINT NOT NULL AUTO_INCREMENT, "
					+ "plugin_name VARCHAR(120) NOT NULL, "
					+ "management_began TIMESTAMP NOT NULL DEFAULT NOW(), "
					+ "current_migration_number INT NOT NULL, "
					+ "last_migration TIMESTAMP, "
					+ "CONSTRAINT pk_managed_plugin_data PRIMARY KEY (managed_id), "
					+ "CONSTRAINT uniq_managed_plugin UNIQUE (plugin_name), "
					+ "INDEX idx_managed_plugin USING BTREE (plugin_name)" +
					");";
	
	private static final String CHECK_LAST_MIGRATION = 
			"SELECT current_migration_number FROM managed_plugin_data WHERE plugin_name = ?;";
	
	private static final String RECORD_MIGRATION =
			"INSERT INTO managed_plugin_data (plugin_name, current_migration_number, last_migration) " 
					+ "VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE plugin_name = VALUES(plugin_name), "
					+ "current_migration_number = VALUES(current_migration_number), "
					+ "last_migration = VALUES(last_migration);";
	
	private static final String CHECK_CREATE_LOCK_TABLE = 
			"CREATE TABLE IF NOT EXISTS managed_plugin_locks ("
					+ "plugin_name VARCHAR(120) NOT NULL, "
					+ "lock_time TIMESTAMP NOT NULL DEFAULT NOW(), "
					+ "CONSTRAINT pk_managed_plugin_locks PRIMARY KEY (plugin_name)" +
					");";
	
	private static final String CLEANUP_LOCK_TABLE = 
			"DELETE FROM managed_plugin_locks WHERE lock_time <= TIMESTAMPADD(HOUR, -8, NOW());";
	
	private static final String ACQUIRE_LOCK = 
			"INSERT IGNORE INTO managed_plugin_locks (plugin_name) VALUES (?);";
	
	private static final String RELEASE_LOCK =
			"DELETE FROM managed_plugin_locks WHERE plugin_name = ?";

	private int firstMigration;
	private TreeMap<Integer, Migration> migrations;
	private int lastMigration;
	
	private ExecutorService postExecutor;

	/**
	 * Create a new ManagedDatasource.
	 * <br /><br />
	 * After creating, a plugin should register its migrations, which are just numbered "sets" of queries to run.
	 * <br /><br />
	 * Use {@link #registerMigration(int, boolean, Callable, String...)} to add a new migration.
	 * <br /><br />
	 * When you are done adding, call {@link #updateDatabase()} which gets a lock on migrating for this
	 *   plugin, then checks if any migrations need to be applied, and applies as needed.
	 * <br /><br />
	 * Now, your database connection pool will be ready to use! 
	 * <br /><br />
	 * Don't worry about "pre-preparing" statements. Just use the following pattern:
	 * <br /><br />
	 * <code>
	 *   try (Connection connection = myManagedDatasource.getConnection();
	 *   		PreparedStatement statement = connection.prepareStatement("SELECT * FROM sample;");) {
	 *   	// code that uses `statement`
	 *   } catch (SQLException se) {
	 *   	// code that alerts on failure
	 *   }
	 * </code>
	 * <br /><br />
	 * Or similar w/ normal Statements. This is a try-with-resources block, and it ensures that once
	 *   the query is complete (even if it errors!) all resources are "closed". In the case of the 
	 *   connection pool, this just returns the connection back to the connection pool for use
	 *   elsewhere.
	 * <br /><br />
	 * If you want to batch, just use a PreparedStatement as illustrated above, and use 
	 *   <code>.addBatch();</code> on it after adding each set of parameters. When you are done,
	 *   call <code>.executeBatch();</code> and all the statements will be executed in order.
	 *   Be sure to watch for errors or warnings and of course read the PreparedStatement API docs
	 *   for any further questions.
	 * 
	 * @param plugin The ACivMod that this datasource backs
	 * @param user The SQL user to connect with
	 * @param pass The password to connect with
	 * @param host The host name / IP of the database server
	 * @param port The port to connection to
	 * @param database The name of the database file
	 * @param poolSize The max # of concurrent connections available in the pool
	 * @param connectionTimeout The length of time to wait for an active query to return
	 * @param idleTimeout The length of time a connection can wait unused before being recycled
	 * @param maxLifetime The absolute length of time a connection is allowed to live in the pool.
	 */
	public ManagedDatasource(ACivMod plugin, String user, String pass, String host, int port, String database,
			int poolSize, long connectionTimeout, long idleTimeout, long maxLifetime) {
		this(plugin, constructStandardConfig(user, pass, host, port, database, poolSize, 
				connectionTimeout, idleTimeout, maxLifetime));
	}
	
	/**
	 * Easy utility method to construct a standard HikariConfig for MySQL databases.
	 * Some defaults will be applied if advanced parameters are missing, but if user, host, port, or database
	 * aren't set, a runtime exception will result.
	 * <br /><br />
	 * Note this does configure the prepared statement cache, assuming we're using a driver that supports it.
	 * By default, the cache size is 250 and the maximum size of the statement we'll cache is 2048 characters.
	 * <br /><br />
	 * If these options won't work for you, make a HikariConfig of your own devising.
	 *  
	 * @param user The SQL user to connect with
	 * @param pass The password to connect with
	 * @param host The host name / IP of the database server
	 * @param port The port to connection to
	 * @param database The name of the database file
	 * @param poolSize The max # of concurrent connections available in the pool (if null, 10)
	 * @param connectionTimeout The length of time to wait for an active query to return (if null, 1000)
	 * @param idleTimeout The length of time a connection can wait unused before being recycled (if null, 600000)
	 * @param maxLifetime The absolute length of time a connection is allowed to live in the pool (if null, 7200000)
	 * @return A new HikariConfig for the described datasource.
	 */
	public static HikariConfig constructStandardConfig(String user, String pass, String host, int port, String database,
			Integer poolSize, Long connectionTimeout, Long idleTimeout, Long maxLifetime) {
		if (user != null && host != null && port > 0 && database != null) {
			HikariConfig config = new HikariConfig();
			config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
			if (poolSize == null) poolSize = 10;
			if (connectionTimeout == null) connectionTimeout = 1000l;
			if (idleTimeout == null) idleTimeout = 600000l;
			if (maxLifetime == null) maxLifetime = 7200000l;
			config.setConnectionTimeout(connectionTimeout);
			config.setIdleTimeout(idleTimeout);
			config.setMaxLifetime(maxLifetime);
			config.setMaximumPoolSize(poolSize);
			config.setUsername(user);
			config.addDataSourceProperty("cachePrepStmts", "true");
			config.addDataSourceProperty("prepStmtCacheSize", "250");
			config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			if (pass != null) {
				config.setPassword(pass);
			}
			return config;
		} else {
			throw new RuntimeException("Incomplete configuration offered");
		}
	}
	
	/**
	 * For more advanced users; provide a custom HikariConfig configuration directly.
	 * 
	 * @see #ManagedDatasource(ACivMod, String, String, String, int, String, int, long, long, long) for usage details.
	 * 
	 * @param plugin The ACivMod that this datasource backs 
	 * @param configuration The {@link HikariConfig} to use in building this datasource.
	 */
	public ManagedDatasource(ACivMod plugin, HikariConfig configuration) {
		this.plugin = plugin;
		this.logger = plugin.getLogger();

		if (configuration != null) {
			this.config = configuration;
	
			this.connections = new HikariDataSource(this.config);
			
			// quick sanity test.
			try (Connection connection = getConnection();
					Statement statement = connection.createStatement();) {
				statement.executeQuery("SELECT 1");
			} catch (SQLException se) {
				logger.log(Level.SEVERE, "Unable to initialize Database", se);
				this.connections = null;
			}
		} else {
			this.connections = null;
			logger.log(Level.SEVERE, "Database not configured and is unavaiable");
		}
		
		this.firstMigration = Integer.MAX_VALUE;
		this.migrations = new TreeMap<Integer, Migration>();
		this.lastMigration = Integer.MIN_VALUE;

		this.postExecutor = Executors.newSingleThreadExecutor();
		
		getReady();
	}
	
	private final void getReady() {
		try (Connection connection = connections.getConnection();) {
			// Try-create migrations table
			try (Statement statement = connection.createStatement();) {
				statement.executeUpdate(ManagedDatasource.CHECK_CREATE_MIGRATIONS_TABLE);
			}
			
			try (Statement statement = connection.createStatement();) {
				statement.executeUpdate(ManagedDatasource.CHECK_CREATE_LOCK_TABLE);
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Failed to prepare migrations table or register this plugin to it.");
		}
	}
	
	/**
	 * @see #registerMigration(int, boolean, Callable, String...)
	 * 
	 * @param migration
	 * @param ignoreErrors
	 * @param query
	 */
	public void registerMigration(int migration, boolean ignoreErrors, String...query) {
		registerMigration(migration, ignoreErrors, null, query);
	}

	/**
	 * ACivMod's should call this to register their migration code. After migrations are registered,
	 * host plugins can call the {@link #updateDatabase} method to trigger each migration in turn.
	 * <br /><br />
	 * This is _not_ checked for completeness or accuracy.
	 *  
	 * @param migration The migration ID -- 0, 1, 2 etc.
	 * @param ignoreErrors indicates if errors in this migration should be ignored.
	 * @param postCall A "Callable" to run after the migration SQL is run, but before the next migration begins.
	 *        Optional. Leave null to do nothing.
	 * @param query The queries to run, in sequence
	 */
	public void registerMigration(int migration, boolean ignoreErrors, Callable<Boolean> postCall, String...query) {
		this.migrations.put(migration, new Migration(ignoreErrors, postCall, query));
		if (migration > lastMigration) lastMigration = migration;
		if (migration < firstMigration) firstMigration = migration;
	}
	
	/**
	 * This method should be called by your plugin after all migrations have been registered.
	 *   It applies the migrations if necessary in a "multi-tenant" safe way via a soft-lock.
	 *   Locks have a maximum duration currently set to 8 hours, but realistically they will
	 *   be very short. For multi-tenant updates all servers should gracefully wait in line.
	 * <br /><br />
	 * <ol>
	 * <li> Attempts to get a lock for migrations for this plugin.
	 * <li> If unsuccessful, periodically check the lock for release.
	 *    <ol><li> Once released, restart at 1.
	 *    <li> If Timeout occurs, return "false".
	 * </ol><li> If successful, check for current update level.
	 *    <ol><li> If no record exists, start with first migration, and apply
	 *       in sequence from first to last, updating the migration management
	 *       table along the way
	 *    <li> If a record exists, read which migration was completed last
	 *       <ol><li>   If identical to "highest" registered migration level, do nothing.
	 *       <li>  If less then "highest" registered migration level,
	 *            get the tailset of migrations "after" the last completed level, and run.
	 * </ol></ol><li> If no errors occurred, or this migration has errors marked ignored, return true.
	 * <li> If errors, return false.
	 * <li> In either case, release the lock.
	 * </ol>
	 * 
	 * @return As described in the algorithm above, returns true if no errors or all ignored; 
	 *    or false if unable to start migration in a timely fashion or errors occurred.
	 */
	public boolean updateDatabase() {
		try {
			checkWaitLock();
		} catch (SQLException se) {
			logger.log(Level.SEVERE, "An uncorrectable SQL error was encountered!", se);
			return false;
		} catch (TimeoutException te) {
			logger.log(Level.SEVERE, "Unable to acquire a lock!", te);
			return false;
		}
		
		// Now check update level
		// etc.
		
		Integer currentLevel = migrations.firstKey() - 1; 
		
		try (Connection connection = getConnection();
				PreparedStatement statement = connection.prepareStatement(CHECK_LAST_MIGRATION); ){
			statement.setString(1, plugin.getName());
			try (ResultSet set = statement.executeQuery();) {
				if (set.next()) {
					currentLevel = set.getInt(1);
				} // else we aren't tracked yet!
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Unable to check last migration!", e);
			releaseLock();
			return false;
		}
		
		NavigableMap<Integer, Migration> newApply = migrations.tailMap(currentLevel, false);
		
		try {
			if (newApply.size() > 0) {
				logger.log(Level.INFO, "{0} database is behind, {1} migrations found", 
						new Object[] {plugin.getName(), newApply.size()});
				if (doMigrations(newApply)) {
					logger.log(Level.INFO, "{0} fully migrated.", plugin.getName());
				} else {
					logger.log(Level.WARNING, "{0} failed to apply updates.", plugin.getName());
					return false;
				}
			} else {
				logger.log(Level.INFO, "{0} database is up to date.", plugin.getName());
			}
			
			return true;
		} catch (Exception e) {
			logger.log(Level.WARNING, "{0} failed to apply updates for some reason", plugin.getName());
			logger.log(Level.WARNING, "Full exception:", e);
			return false;
		} finally {
			releaseLock();
		}
	}
	
	private boolean doMigrations(NavigableMap<Integer, Migration> newApply) {
		try {
			for (Integer next : newApply.keySet()) {
				logger.log(Level.INFO, "Migration {0} ] Applying", next);
				Migration toDo = newApply.get(next);
				if (toDo == null) continue; // huh?
				if (doMigration(next, toDo.migrations, toDo.ignoreErrors, toDo.postMigration)) {
					logger.log(Level.INFO, "Migration {0} ] Successful", next);

					try (Connection connection = getConnection();
							PreparedStatement statement = connection.prepareStatement(RECORD_MIGRATION); ){
						statement.setString(1, plugin.getName());
						statement.setInt(2, next);
						if (statement.executeUpdate() < 1) {
							logger.log(Level.WARNING, "Might not have recorded migration {0} occurrence successfully.", next);
						}
					} catch (SQLException e) {
						logger.log(Level.SEVERE, "Failed to record migration {0} occurrence successfully.", next);
						logger.log(Level.SEVERE, "Full Error: ", e);
						return false;
					}
					
					
				} else {
					logger.log(Level.INFO, "Migration {0} ] Failed.", next);
					return false;
				}
			}
			
			return true;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unexpected failure during migrations", e);
			return false;
		}
		
	}
	
	private boolean doMigration(Integer migration, LinkedList<String> queries, boolean ignoreErrors, Callable<Boolean> post) {
		try (Connection connection = getConnection();) {
			for (String query : queries) {
				try (Statement statement = connection.createStatement();){
					statement.executeUpdate(query);
					
					if (!ignoreErrors) { // if we ignore errors we totally ignore warnings.
						SQLWarning warning = statement.getWarnings();
						while (warning != null) {
							logger.log(Level.WARNING, "Migration {0} ] Warning: {1}",
									new Object[] {migration, warning.getMessage()});
							// TODO: add verbose check
							warning = warning.getNextWarning();
						}
					}
				} catch (SQLException se) {
					if (ignoreErrors) {
						logger.log(Level.WARNING, "Migration {0} ] Ignoring error: {1}", 
								new Object[] {migration, se.getMessage()});
					} else {
						throw se;
					}
				}
			}
		} catch (SQLException se) {
			if (ignoreErrors) {
				logger.log(Level.WARNING, "Migration {0} ] Ignoring error: {1}", 
						new Object[] {migration, se.getMessage()});
			} else {
				logger.log(Level.SEVERE, "Migration {0} ] Failed migration: {1}",
						new Object[] {migration, se.getMessage()});
				logger.log(Level.SEVERE, "Full Error:", se);
				return false;
			}
		}

		if (post != null) {
			Future<Boolean> doing = postExecutor.submit(post);

			try {
				if (doing.get()) {
					logger.log(Level.INFO, "Migration {0} ] Post Call Complete", migration);
				} else {
					if (ignoreErrors) {
						logger.log(Level.WARNING, "Migration {0} ] Post Call indicated failure; ignored.", migration);
					} else {
						logger.log(Level.SEVERE, "Migration {0} ] Post Call failed!", migration);
						return false;
					}
				}
			} catch (Exception e) {
				if (ignoreErrors) {
					logger.log(Level.WARNING, "Migration {0} ] Post Call indicated failure; ignored: {1}", 
							new Object[] {migration, e.getMessage()});
				} else {
					logger.log(Level.SEVERE, "Migration {0} ] Post Call failed!", migration);
					logger.log(Level.SEVERE, "Full Error:", e);
					return false;
				}
			}

		}
		return true;
	}

	/**
	 * This attempts to acquire a lock every WAIT_PERIOD milliseconds, up to MAX_WAIT_FOR_LOCK milliseconds.
	 * <br /><br />
	 * If max wait is exhausted, throws a TimeoutException.
	 * <br /><br />
	 * If a <i>real</i> error (not failure to insert) is encountered, stops trying and throws that error.
	 * <br /><br />
	 * Otherwise, returns true when lock is acquired.
	 * 
	 * @return true when lock is acquired, or exception otherwise
	 * @throws TimeoutException If lock isn't acquired by max wait time.
	 * @throws SQLException If an exception is encountered
	 */
	private boolean checkWaitLock() throws TimeoutException, SQLException {
		/* First, cleanup old locks if any */
		try (Connection connection = getConnection();
				Statement cleanup = connection.createStatement();) {
			cleanup.executeUpdate(CLEANUP_LOCK_TABLE);
		} catch (SQLException se) {
			logger.log(Level.SEVERE, "Unable to cleanup old locks, error encountered!");
			throw se;
		}
		/* Now get our own lock */
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() - start < MAX_WAIT_FOR_LOCK) {
			try (Connection connection = getConnection();
					PreparedStatement tryAcquire = connection.prepareStatement(ACQUIRE_LOCK);) {
				tryAcquire.setString(1, plugin.getName());
				int havelock = tryAcquire.executeUpdate();
				if (havelock > 0) {
					logger.log(Level.INFO, "Lock acquired, proceeding.");
					return true;
				}
			} catch (SQLException failToAcquire) {
				logger.log(Level.SEVERE, "Unable to acquire a lock, error encountered!");
				throw failToAcquire; // let the exception continue so we return right away; only errors we'd encounter here are terminal.
			}
			
			if (System.currentTimeMillis() - start > MAX_WAIT_FOR_LOCK) break;

			try {
				Thread.sleep(WAIT_PERIOD);
			} catch (InterruptedException ie) {
				// Someone wants us to check right away.
			}
		}
		throw new TimeoutException("We were unable to acquire a lock in the time allowed");
	}
	

	private void releaseLock() {
		try (Connection connection = getConnection();
				PreparedStatement release = connection.prepareStatement(RELEASE_LOCK);) {
			release.setString(1, plugin.getName());
			int releaseLock = release.executeUpdate();
			if (releaseLock < 1) {
				logger.log(Level.WARNING, "Attempted to release a lock, already released.");
			} else {
				logger.log(Level.INFO, "Lock released.");
			}
		} catch (SQLException fail) {
			logger.log(Level.WARNING, "Attempted to release lock; failed. This may interrupt startup for other servers working against this database.", fail);
		}
	}
	
	/**
	 * Passthrough; gets a connection from the underlying HikariDatasource.
	 * <br /><br />
	 * Simply close() it when done.
	 * <br /><br />
	 * This method _could_ briefly block while waiting for a connection. Keep this in mind.
	 * 
	 * @return A {@link java.sql.Connection} connection from the pool.
	 * @throws SQLException If the pool has gone away, database is not connected, or other
	 *   error in retrieving a connection.
	 */
	public Connection getConnection() throws SQLException {
		available();
		return this.connections.getConnection();
	}
	
	/** 
	 * Closes all connections and this connection pool.
	 * 
	 * @throws SQLExceptionsomething went horribly wrong.
	 */
	public void close() throws SQLException {
		available();
		this.connections.close();
		this.connections = null;
	}
	
	/**
	 * Quick test; either ends or throws an exception if data source isn't configured.
	 * 
	 * @throws SQLException
	 */
	public void available() throws SQLException {
		if (this.connections == null) {
			throw new SQLException("No Datasource Available");
		}
	}
	
	/**
	 * Gets the underlying {@link HikariDataSource} object, if you need more advanced controls.
	 * 
	 * @return The real datasource; not a copy, not wrapped, so be careful.
	 */
	public HikariDataSource getDataSource() {
		return this.connections;
	}
	
	/**
	 * Gets the underlying (@link HikariConfig} object, if you need advanced inspection.
	 *
	 * @return The real config; not a copy, not wrapped, be careful.
	 */
	public HikariConfig getConfig() {
		return this.config;
	}
	
	private static class Migration {
		public LinkedList<String> migrations;
		public boolean ignoreErrors;
		public Callable<Boolean> postMigration;

		public Migration(boolean ignoreErrors, Callable<Boolean> postMigration, String ... migrations) {
			this.migrations = new LinkedList<String>(Arrays.asList(migrations));
			this.ignoreErrors = ignoreErrors;
			this.postMigration = postMigration;
		}	
	}	
}