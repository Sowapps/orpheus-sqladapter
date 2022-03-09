<?php
/**
 * Library to easily use PDO
 *
 * @author Florent Hazard <contact@sowapps.com>
 * @copyright The MIT License, see LICENSE.txt
 *
 * Library of PDO functions to easily use DBMS.
 *
 * Useful constants:
 * LOGS_PATH
 * PDOLOGFILENAME
 *
 * Required functions:
 * bintest() (Core lib)
 */

use Orpheus\Config\IniConfig;
use Orpheus\SqlAdapter\Exception\SqlException;
use Orpheus\SqlAdapter\SqlAdapter;


defifn('DBCONF', 'database');

//Constantes PDO
define('PDOQUERY', 0);//Simple Query (SELECT ...). Returns a result set.
define('PDOEXEC', 1);//Simple Execution (INSERT INTO, UPDATE, DELETE ...). Returns the number of affected lines.

define('PDONOSTMT', PDOQUERY | 0 << 1);//Continue, can not be used alone.
define('PDOSTMT', PDOQUERY | 1 << 1);//Returns the PDOStatement without any treatment but does NOT free the connection.
define('PDOFETCH', PDOQUERY | 0 << 2);//Query and simple Fetch (only one result) - Default
define('PDOFETCHALL', PDOQUERY | 1 << 2);//Query and Fetch All (Set of all results)
define('PDOFETCHALLCOL', PDOQUERY | 0 << 3);//All columns
define('PDOFETCHFIRSTCOL', PDOQUERY | 1 << 3);//Only the first column

define('PDOERROR_FATAL', 0 << 10);
define('PDOERROR_MINOR', 1 << 10);

/**
 * Get the default instance's name
 *
 * @return string
 */
function pdo_getDefaultInstance() {
	global $DBS;
	if( defined('PDODEFINSTNAME') ) {
		// Default is constant PDODEFINSTNAME
		$instance = PDODEFINSTNAME;
		
	} elseif( !empty($DBS) && is_array($DBS) ) {
		if( is_array(current($DBS)) ) {
			// Default is the first value of the multidimensional array DB Settings
			$instance = key($DBS);
		} else {
			// Default is 'default' and value is all the contents of DB Settings
			$instance = 'default';
			$DBS[$instance] = $DBS;
		}
	} else {
		pdo_error('Database configuration not found, define constant PDODEFINSTNAME to set the default instance.', 'Instance Definition');
	}
	return $instance;
}

/**
 * Load DB config from config file
 */
function pdo_loadConfig() {
	global $DBS;
	//Check DB Settings File and Get DB Settings
	if( empty($DBS) ) {
		$DBS = IniConfig::build(DBCONF, true, false);
		$DBS = $DBS->all;
	}
}

/**
 * Get setting of $instance
 *
 * @param string $instance
 * @return array
 */
function pdo_getSettings($instance = null): array {
	global $DBS;
	$instance = $instance ?: pdo_getDefaultInstance();
	// Load instance settings
	$instanceSettings = $DBS[$instance];
	if( $instanceSettings['driver'] !== 'sqlite' ) {
		if( empty($instanceSettings['host']) ) {
			$instanceSettings['host'] = '127.0.0.1';
		}
		if( empty($instanceSettings['user']) ) {
			$instanceSettings['user'] = 'root';
		}
		if( empty($instanceSettings['passwd']) ) {
			$instanceSettings['passwd'] = '';
		}
	}
	
	return $instanceSettings;
}

function pdo_checkInstanceName(&$instance) {
	pdo_loadConfig();
	
	$instance = null;
	// Using default instance
	if( empty($instance) ) {
		// Get from default
		$instance = pdo_getDefaultInstance();
		
	} elseif( empty($DBS[$instance]) ) {
		pdo_error('Parameter Instance " ' . $instance . ' " is unknown.', 'Instance Setting Definition');
	}
	
	return $instance;
}

/**
 * Ensure $instance is connected to DBMS
 *
 * @param string $instance If supplied, this is the ID of the instance to use to execute the query. Optional, PDODEFINSTNAME constant by default.
 * @return string Instance ID used
 *
 * Ensure to provide a valid and connected instance of PDO, here are the steps:
 * If it is not loaded, this function attempts to load the database configuration file.
 * If not supplied as a parameter, this function attempts to determine an existing instance name.
 * If the instance is not connected, this function attempts to connect.
 */
function ensure_pdoinstance($instance = null) {
	global $pdoInstances;
	
	pdo_checkInstanceName($instance);
	
	if( !empty($pdoInstances[$instance]) ) {
		// Instance is already checked and loaded
		return $instance;
	}
	
	$instanceSettings = pdo_getSettings($instance);
	
	try {
		$pdoInstances[$instance] = pdo_connect($instanceSettings);
	} catch( PDOException $e ) {
		pdo_error('PDO Exception: ' . $e->getMessage(), 'DB Connection', 0, $e);
	}
	
	return $instance;
}

/**
 * @throws SqlException
 */
function pdo_connect($settings, $selectDatabase = true): ?PDO {
	// If There is no driver given, it is an error.
	if( empty($settings['driver']) ) {
		pdo_error('Database setting "driver" should have the driver name (not empty)', 'Driver Definition');
		
		//If driver is mysql
	} elseif( $settings['driver'] === 'mysql' ) {
		//If Instance does not exist yet, it is not connected, we create it & link it.
		if( $selectDatabase && empty($settings['dbname']) ) {
			pdo_error('Database setting "dbname" should have the database\'s name (not empty)', 'DB Name Definition');
		}
		$instance = new PDO(
			"mysql:host={$settings['host']}"
			. ($selectDatabase ? ';dbname=' . $settings['dbname'] : '')
			. (!empty($settings['port']) ? ';port=' . $settings['port'] : ''),
			$settings['user'], $settings['passwd'],
			[PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES utf8", PDO::MYSQL_ATTR_DIRECT_QUERY => true]
		);
		$instance->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		
		//If driver is mssql
	} elseif( $settings['driver'] === 'mssql' ) {
		//If Instance does not exist yet, it is not connected, we create it & link it.
		if( $selectDatabase && empty($settings['dbname']) ) {
			pdo_error('Database setting "dbname" should have the database\'s name (not empty)', 'DB Name Definition');
		}
		$instance = new PDO(
			"dblib:host={$settings['host']}"
			. ($selectDatabase ? ';dbname=' . $settings['dbname'] : '')
			. (!empty($settings['port']) ? ':' . $settings['port'] : ''),
			$settings['user'], $settings['passwd']
		);
		$instance->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		
	} elseif( $settings['driver'] === 'pgsql' ) {
		//If Instance does not exist yet, it is not connected, we create it & link it.
		if( $selectDatabase && empty($settings['dbname']) ) {
			pdo_error('Database setting "dbname" should have the database\'s name (not empty)', 'DB Name Definition');
		}
		$instance = new PDO(
			"pgsql:host={$settings['host']}"
			. ($selectDatabase ? ';dbname=' . $settings['dbname'] : '')
			. (!empty($settings['port']) ? ';port=' . $settings['port'] : '')
			. ";user={$settings['user']};password={$settings['passwd']}"
		);
		$instance->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		
	} elseif( $settings['driver'] === 'sqlite' ) {
		//If Instance does not exist yet, it is not connected, we create it & link it.
		$settings['path'] = empty($settings['path']) ? ':memory:' : $settings['path'];
		$instance = new PDO(
			"sqlite:{$settings['path']}"
		);
		$instance->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
	} else {
		pdo_error('Database setting "driver" does not match any known driver', 'Driver Definition');
	}
	
	return $instance ?? null;
}

/**
 * Execute $query
 *
 * Execute $query on the instantiated database.
 *
 * @param string $query The query to execute.
 * @param int $fetch See PDO constants above. Optional, default is PDOQUERY.
 * @param string $instance The instance to use to execute the query. Optional, default is defined by ensure_pdoinstance().
 * @return mixed The result of the query, of type defined by $fetch.
 */
function pdo_query($query, $fetch = PDOQUERY, $instance = null) {
	global $pdoInstances, $DBS;
	// Checks connection
	if( $instance instanceof SqlAdapter ) {
		$pdoInstance = $instance->getPdo();
		$driver = $instance->getDriver();
	} else {
		$instance = ensure_pdoinstance($instance);
		if( empty($pdoInstances[$instance]) ) {
			return null;
		}
		$instanceSettings = $DBS[$instance];
		$pdoInstance = $pdoInstances[$instance];
		$driver = $instanceSettings['driver'];
	}
	
	if( in_array($driver, ['mysql', 'dblib', 'pgsql', 'sqlite']) ) {
		
		try {
			$ERR_ACTION = 'BINTEST';
			if( bintest($fetch, PDOEXEC) ) {// Exec
				$ERR_ACTION = 'EXEC';
				return $pdoInstance->exec($query);
			}
			$ERR_ACTION = 'QUERY';
			$PDOSQuery = $pdoInstance->query($query);
			$returnValue = null;
			if( bintest($fetch, PDOSTMT) ) {
				return $PDOSQuery;
				
			} elseif( bintest($fetch, PDOFETCHALL) ) {
				$ERR_ACTION = 'FETCHALL';
				if( bintest($fetch, PDOFETCHFIRSTCOL) ) {
					$returnValue = $PDOSQuery->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_COLUMN, 0);
				} else {
					$returnValue = $PDOSQuery->fetchAll(PDO::FETCH_ASSOC);
				}
				
			} elseif( bintest($fetch, PDOFETCH) ) {
				$ERR_ACTION = 'FETCH';
				if( bintest($fetch, PDOFETCHFIRSTCOL) ) {
					$returnValue = $PDOSQuery->fetchColumn(0);
				} else {
					$returnValue = $PDOSQuery->fetch(PDO::FETCH_ASSOC);
				}
				$PDOSQuery->fetchAll();
			}
			$PDOSQuery->closeCursor();
			unset($PDOSQuery);
			
			return $returnValue;
		} catch( PDOException $e ) {
			pdo_error($ERR_ACTION . ' ERROR: ' . $e->getMessage(), 'Query: ' . $query, $fetch, $e);
			
			return false;
		}
	}
	// Unknown Driver
	pdo_error(sprintf('Driver "%s" does not exist or is not implemented yet.', $driver), 'Driver Definition');
	
	return null;
}

/**
 * Log a PDO error
 *
 * Save the error report $report in the log file and throw an exception.
 * If the error is minor, nothing happen, else
 * The error is reported and an exception is thrown
 *
 * @param string $report The report to save.
 * @param string $action Optional information about what the script was doing.
 * @param int $fetch The fetch flags, if PDOERROR_MINOR, this function does nothing. Optional, default value is 0.
 * @param PDOException $original The original exception. Optional, default value is null.
 * @throws SqlException
 */
function pdo_error($report, $action = '', $fetch = 0, $original = null) {
	if( bintest($fetch, PDOERROR_MINOR) ) {
		return;
	}
	sql_error($report, $action, true);
	throw new SqlException($report, $action, $original);
}
