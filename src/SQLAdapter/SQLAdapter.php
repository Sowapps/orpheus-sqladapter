<?php
/**
 * SQLAdapter
 */

namespace Orpheus\SQLAdapter;

use Exception;
use Orpheus;
use Orpheus\Cache\APCache;
use Orpheus\Config\IniConfig;
use Orpheus\SQLAdapter\Exception\SQLException;
use PDO;

/**
 * The main SQL Adapter class
 *
 * This class is the mother sql adapter inherited for specific DBMS.
 */
abstract class SQLAdapter {
	
	const OBJECT = 1;
	const ARR_FIRST = 2;
	const ARR_ASSOC = 3;
	const ARR_OBJECTS = 4;
	const STATEMENT = 5;
	const SQLQUERY = 6;
	
	//List of outputs for getting list
	const NUMBER = 7;//!< Object
	
	/**
	 * Select defaults options
	 *
	 * @var array
	 */
	protected static $selectDefaults = [];//!< First element only (from ARR_ASSOC)
	/**
	 * Update defaults options
	 *
	 * @var array
	 */
	protected static $updateDefaults = [];//!< Array of associative arrays
	/**
	 * Delete defaults options
	 *
	 * @var array
	 */
	protected static $deleteDefaults = [];//!< Array of objects
	/**
	 * Insert defaults options
	 *
	 * @var array
	 */
	protected static $insertDefaults = [];//!< SQL Statement
	/**
	 * All Adapter instances by name
	 *
	 * @var array
	 */
	protected static $instances = [];//!< Query String
	/**
	 * Store drivers' adapter
	 *
	 * @var array
	 */
	protected static $adapters = [
		'mysql' => 'Orpheus\SQLAdapter\SQLAdapterMySQL',
		'mssql' => 'Orpheus\SQLAdapter\SQLAdapterMSSQL',
	];
	
	/**
	 * Configurations
	 *
	 * @var array
	 */
	protected static $configs;
	
	/**
	 * The ID field
	 *
	 * @var string
	 */
	protected $IDFIELD = 'id';
	
	/**
	 * The PDO instance
	 *
	 * @var PDO
	 */
	protected $pdo;
	
	/**
	 * Constructor
	 *
	 * @param string $name The name of the instance
	 * @param mixed $config Instance config to use, maybe a config name, a config array or a PDO instance
	 */
	public function __construct($name, $config) {
		
		if( is_object($name) ) {
			
			// Deprecated, retrocompatibility
			// TODO Remove this case
			$this->pdo = $name;
			
			// If is array ?
		} else {
			
			$this->connect($config + static::getDefaults());
			static::registerInstance($name, $this);
		}
	}
	
	/**
	 * Connect to the DBMS using $config
	 *
	 * @param array $config
	 */
	protected abstract function connect(array $config);
	
	/**
	 * Get defaults configuration to fill missing options
	 *
	 * @return string[]
	 */
	protected static function getDefaults() {
		return [
			'host'   => '127.0.0.1',
			'user'   => 'root',
			'passwd' => '',
		];
	}
	
	/**
	 * Register an unique instance by its name
	 *
	 * @param string $name
	 * @param Orpheus\SQLAdapter\SQLAdapter $adapter
	 */
	protected static function registerInstance($name, $adapter) {
		static::$instances[$name] = $adapter;
	}
	
	/**
	 * Query the DB server
	 *
	 * @param string $Query The query to execute.
	 * @param int $Fetch See PDO constants above. Optional, default is PDOQUERY.
	 * @return mixed The result of pdo_query()
	 * @see pdo_query()
	 */
	public function query($Query, $Fetch = PDOQUERY) {
		return pdo_query($Query, $Fetch, $this);
	}
	
	/**
	 * Select something from database
	 *
	 * @param array $options The options used to build the query
	 * @return mixed Mixed return, depending on the 'output' option
	 */
	public abstract function select(array $options = []);
	
	/**
	 * Update something in database
	 *
	 * @param array $options The options used to build the query
	 * @return int The number of affected rows
	 */
	public abstract function update(array $options = []);
	
	/**
	 * Insert something in database
	 *
	 * @param array $options The options used to build the query
	 * @return int The number of inserted rows
	 */
	public abstract function insert(array $options = []);
	
	/**
	 * Delete something in database
	 *
	 * @param array $options The options used to build the query
	 * @return int The number of deleted rows
	 */
	public abstract function delete(array $options = []);
	
	/**
	 * Get the last inserted ID
	 *
	 * @param string $table The table to get the last inserted id
	 * @return mixed The last inserted id value
	 *
	 * It requires a successful call of insert() !
	 */
	public function lastID($table) {
		return $this->pdo->lastInsertId();
	}
	
	/**
	 * Format a list of values
	 *
	 * @param array $list
	 * @return string
	 */
	public function formatValueList(array $list) {
		$string = '';
		foreach( $list as $i => $v ) {
			$string .= ($i ? ',' : '') . $this->formatValue($v);
		}
		return $string;
	}
	
	/**
	 * Format SQL value
	 *
	 * @param string $value The value to format.
	 * @return string The formatted value.
	 *
	 * Format the given value to the matching SQL type.
	 * If the value is a float, we make french decimal compatible with SQL.
	 * If null, we use the NULL value, else we consider it as a string value.
	 */
	public function formatValue($value) {
		return $this->escapeValue($value);
	}
	
	/**
	 * Escape SQL value
	 *
	 * @param string $value The value to format.
	 * @return string The formatted value.
	 * @see formatValue()
	 *
	 * Escape the given value to the matching SQL type.
	 * If the value is a float, we make french decimal compatible with SQL.
	 * If null, we use the NULL value, else we consider it as a string value.
	 */
	public function escapeValue($value) {
		return $value === null ? 'NULL' : $this->formatString($value);
	}
	
	/**
	 * Format SQL string
	 *
	 * @param string $str The string to format.
	 * @return string The formatted string.
	 *
	 * Format the given string as an SQL string.
	 */
	public function formatString($str) {
		return "'" . str_replace("'", "''", "$str") . "'";
	}
	
	/**
	 * Set the IDFIELD
	 *
	 * @param string $field The new ID field.
	 * @return \Orpheus\SQLAdapter\SQLAdapter
	 *
	 * Set the IDFIELD value to $field
	 */
	public function setIDField($field) {
		if( $field !== null ) {
			$this->IDFIELD = $field;
		}
		return $this;
	}
	
	/**
	 * Format the given $fields into an escaped SQL string list of key=value
	 *
	 * @param array|string $fields
	 * @return string
	 */
	protected function formatFieldList($fields) {
		if( !is_array($fields) ) {
			return $fields;
		}
		$string = '';
		foreach( $fields as $key => $value ) {
			$string .= ($string ? ', ' : '') . $this->escapeIdentifier($key) . '=' . $this->formatValue($value);
		}
		return $string;
	}
	
	/**
	 * Escape SQL identifiers
	 *
	 * @param string $identifier The identifier to escape
	 * @return string The escaped identifier
	 *
	 * Escapes the given string as an SQL identifier.
	 */
	public function escapeIdentifier($identifier) {
		return '"' . $identifier . '"';
	}
	
	/**
	 * Get an unique instance of SQLAdapter by its name
	 *
	 * @param string $name Name of the instance, default value is "default"
	 * @return Orpheus\SQLAdapter\SQLAdapter
	 * @throws SQLException
	 */
	public static function getInstance($name = null) {
		if( !$name ) {
			$name = 'default';
		}
		if( !isset(static::$instances[$name]) ) {
			static::make($name);
		}
		return static::$instances[$name];
	}
	
	/**
	 * Try to make a SQLAdapter by its name loading from configuration
	 *
	 * @param string $name
	 * @return Orpheus\SQLAdapter\SQLAdapter
	 * @throws SQLException
	 */
	public static function make($name = 'default') {
		$configs = static::listConfig();
		
		if( !isset($configs[$name]) ) {
			throw new SQLException('Database configuration with name "' . $name . '" not found.', 'Loading configuration');
		}
		
		$config = $configs[$name];
		
		if( empty($config['driver']) ) {
			throw new SQLException('Database configuration with name "' . $name . '" has no driver property.', 'Loading configuration');
		}
		
		if( empty(static::$adapters[$config['driver']]) ) {
			throw new SQLException('Database configuration with name "' . $name . '" requires an unknown driver "' . $config['driver'] . '".', 'Loading configuration');
		}
		
		$adapterClass = static::$adapters[$config['driver']];
		return new $adapterClass($name, $config);
		
	}
	
	/**
	 * List all instance's configuration
	 *
	 * @return array
	 */
	public static function listConfig() {
		if( static::$configs !== null ) {
			return static::$configs;
		}
		$cache = new APCache('sqladapter', 'db_configs', 2 * 3600);
		if( !$cache->get($configs) ) {
			$fileCconfig = IniConfig::build(DBCONF, true, false)->all;
			$configs = [];
			foreach( $fileCconfig as $key => $value ) {
				if( is_array($value) ) {
					// Instance config
					$configs[$key] = $value;
					
				} else {
					// Instance config property
					if( !isset($configs['default']) ) {
						$configs['default'] = [];
					}
					$configs['default'][$key] = $value;
				}
			}
			$cache->set($configs);
		}
		return static::$configs = $configs;
	}
	
	/**
	 * Register a driver adapter
	 *
	 * @param string $driver
	 * @param string $class
	 */
	public static function registerAdapter($driver, $class) {
		static::$adapters[$driver] = $class;
	}
	
	/**
	 * The static function to use for SELECT queries in global context
	 *
	 * @param array $options The options used to build the query.
	 * @param string $instance The db instance used to send the query.
	 * @param string $IDField The ID field of the table.
	 * @throws Exception
	 * @deprecated
	 * @see select()
	 */
	public static function doSelect(array $options = [], $instance = null, $IDField = null) {
		self::prepareQuery($options, $instance, $IDField);
		return self::$instances[$instance]->select($options);
	}
	
	/**
	 * Prepare the query for the given instance
	 *
	 * @param array $options The options used to build the query.
	 * @param string $instance The db instance used to send the query.
	 * @param string $IDField The ID field of the table.
	 * @throws Exception
	 */
	public static function prepareQuery(array &$options = [], &$instance = null, $IDField = null) {
		self::prepareInstance($instance);
		self::$instances[$instance]->setIDField($IDField);
		if( !empty($options) && !empty($options['output']) && $options['output'] == SQLAdapter::ARR_FIRST ) {
			$options['number'] = 1;
		}
	}
	
	/**
	 * The static function to prepareInstance an adapter for the given instance
	 *
	 * @param string $instance The db instance name to prepareInstance.
	 * @throws Exception
	 */
	public static function prepareInstance(&$instance = null) {
		if( isset(self::$instances[$instance]) ) {
			return;
		}
		global $DBS;
		$instance = ensure_pdoinstance($instance);
		if( empty($DBS[$instance]) ) {
			throw new Exception("Adapter unable to connect to the database.");
		}
		if( empty(static::$adapters[$DBS[$instance]['driver']]) ) {
			throw new Exception("Adapter not found for driver {$DBS[$instance]['driver']}.");
		}
		$adapterClass = static::$adapters[$DBS[$instance]['driver']];
		// $instance is prepareInstance() name of instance and $instance is the real one
		self::$instances[$instance] = new $adapterClass($instance, $DBS[$instance]);
	}
	
	/**
	 * The static function to use for UPDATE queries in global context
	 *
	 * @param array $options The options used to build the query.
	 * @param string $instance The db instance used to send the query.
	 * @param string $IDField The ID field of the table.
	 * @throws Exception
	 * @deprecated
	 * @see update()
	 */
	public static function doUpdate(array $options = [], $instance = null, $IDField = null) {
		self::prepareQuery($options, $instance, $IDField);
		return self::$instances[$instance]->update($options);
	}
	
	/**
	 * The static function to use for DELETE queries in global context
	 *
	 * @param array $options The options used to build the query.
	 * @param string $instance The db instance used to send the query.
	 * @param string $IDField The ID field of the table.
	 * @see SQLAdapter::delete()
	 * @deprecated
	 */
	public static function doDelete(array $options = [], $instance = null, $IDField = null) {
		self::prepareQuery($options, $instance, $IDField);
		return self::$instances[$instance]->delete($options);
	}
	
	/**
	 * The static function to use for INSERT queries in global context
	 *
	 * @param array $options The options used to build the query.
	 * @param string $instance The db instance used to send the query.
	 * @param string $IDField The ID field of the table.
	 * @throws Exception
	 * @deprecated
	 * @see SQLAdapter::insert()
	 */
	public static function doInsert(array $options = [], $instance = null, $IDField = null) {
		self::prepareQuery($options, $instance, $IDField);
		return self::$instances[$instance]->insert($options);
	}
	
	/**
	 * The static function to use to get last isnert id in global context
	 *
	 * @param string $table The table to get the last ID. Some DBMS ignore it.
	 * @param string $IDField The field id name.
	 * @param string $instance The db instance used to send the query.
	 * @throws Exception
	 * @deprecated
	 * @see SQLAdapter::lastID()
	 */
	public static function doLastID($table, $IDField = 'id', $instance = null) {
		$options = [];
		self::prepareQuery($options, $instance, $IDField);
		return self::$instances[$instance]->lastID($table);
	}
	
	/**
	 * Escapes SQL identifiers
	 *
	 * @param string $Identifier The identifier to escape.
	 * @param string $instance The db instance used to send the query.
	 * @return string The escaped identifier.
	 * @throws Exception
	 * @deprecated
	 *
	 * Escapes the given string as an SQL identifier.
	 * @see SQLAdapter::escapeIdentifier()
	 */
	public static function doEscapeIdentifier($Identifier, $instance = null) {
		self::prepareInstance($instance);
		return self::$instances[$instance]->escapeIdentifier($Identifier);
	}
	
	/**
	 * Escapes SQL identifiers
	 *
	 * @param string $String The value to format.
	 * @param string $instance The db instance used to send the query.
	 * @return string The formatted value.
	 * @throws Exception
	 * @deprecated
	 *
	 * Formats the given value to the matching SQL type.
	 * If the value is a float, we make french decimal compatible with SQL.
	 * If null, we use the NULL value, else we consider it as a string value.
	 * @see SQLAdapter::formatString()
	 */
	public static function doFormatString($String, $instance = null) {
		self::prepareInstance($instance);
		return self::$instances[$instance]->formatString($String);
	}
	
	/**
	 * The static function to quote
	 *
	 * @param string $value The string to quote.
	 * @param string $instance The db instance used to send the query.
	 * @return string The quoted string.
	 * @throws Exception
	 * @deprecated
	 *
	 * Add slashes before simple quotes in $String and surrounds it with simple quotes and .
	 * Keep in mind this function does not really protect your DB server, especially against SQL injections.
	 * @see SQLAdapter::formatValue()
	 */
	public static function doFormatValue($value, $instance = null) {
		self::prepareInstance($instance);
		return self::$instances[$instance]->formatValue($value);
	}
	
	/**
	 * @return PDO
	 */
	public function getPdo() {
		return $this->pdo;
	}
	
	/**
	 * Get the driven string
	 *
	 * @return string
	 */
	public abstract static function getDriver();
}
