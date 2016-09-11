/**
 * Convenient Database handling for shared use by all plugins.
 * 
 * Wraps HikariCP for easy connection pooling.
 * 
 * Plugins should use the {@link ManagedDatasource} class. It provides easy access to the underlying Hikari objects
 * if those are required.
 * 
 * @author ProgrammerDan
 *
 */
package vg.civcraft.mc.civmodcore.dao;