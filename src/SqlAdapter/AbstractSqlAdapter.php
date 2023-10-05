<?php
/**
 * AbstractSqlAdapter
 */

namespace Orpheus\SqlAdapter;

use Orpheus;
use Orpheus\Cache\ApcCache;
use Orpheus\Config\IniConfig;
use Orpheus\SqlAdapter\Adapter\MsSqlAdapter;
use Orpheus\SqlAdapter\Adapter\MySqlAdapter;
use Orpheus\SqlAdapter\Exception\SqlException;
use PDO;
use PDOException;

/**
 * The main SQL Adapter class
 *
 * This class is the mother sql adapter inherited for specific DBMS.
 */
abstract class AbstractSqlAdapter {
	
	const DEFAULT_INSTANCE = 'default';
	const CONFIG = 'database';
	
	const RETURN_OBJECT = 1;
	const RETURN_ARRAY_FIRST = 2;
	const RETURN_ARRAY_ASSOC = 3;
	const ARR_OBJECTS = 4;
	const STATEMENT = 5;
	const SQL_QUERY = 6;
	
	const NUMBER = 7;
	
	public const PROCESS_QUERY = 0; //Simple Query (SELECT ...). Returns a result set.
	
	public const PROCESS_EXEC = 1; //Simple Execution (INSERT INTO, UPDATE, DELETE ...). Returns the number of affected lines.
	//	public const QUERY_OUTPUT_NO_STATEMENT = AbstractSqlAdapter::PROCESS_QUERY | 0 << 1;//Continue, can not be used alone.
	public const QUERY_STATEMENT = AbstractSqlAdapter::PROCESS_QUERY | 1 << 1;//Returns the PDOStatement without any treatment but does NOT free the connection.
	public const QUERY_FETCH_ONE = AbstractSqlAdapter::PROCESS_QUERY | 0 << 2;//Query and simple Fetch (only one result) - Default
	public const QUERY_FETCH_ALL = AbstractSqlAdapter::PROCESS_QUERY | 1 << 2;//Query and Fetch All (Set of all results)
	public const FETCH_ALL_COLUMNS = AbstractSqlAdapter::PROCESS_QUERY | 0 << 3;//All columns
	public const FETCH_FIRST_COLUMN = AbstractSqlAdapter::PROCESS_QUERY | 1 << 3;//Only the first column
	
	/**
	 * Select defaults options
	 *
	 * @var array
	 */
	protected static array $selectDefaults = [];//!< First element only (from ARR_ASSOC)
	
	/**
	 * Update defaults options
	 *
	 * @var array
	 */
	protected static array $updateDefaults = [];//!< Array of associative arrays
	
	/**
	 * Delete defaults options
	 *
	 * @var array
	 */
	protected static array $deleteDefaults = [];//!< Array of objects
	
	/**
	 * Insert defaults options
	 *
	 * @var array
	 */
	protected static array $insertDefaults = [];//!< SQL Statement
	
	/**
	 * All Adapter instances by name
	 *
	 * @var AbstractSqlAdapter[]
	 */
	protected static array $instances = [];//!< Query String
	
	/**
	 * Store drivers' adapter
	 *
	 * @var array
	 */
	protected static array $adapters = [
		'mysql' => MySqlAdapter::class,
		'mssql' => MsSqlAdapter::class,
	];
	
	/**
	 * Configurations
	 */
	protected static ?array $configs = null;
	
	/**
	 * The ID field
	 */
	protected string $defaultIdField = 'id';
	
	protected array $config;
	
	/**
	 * The PDO instance
	 *
	 * @var PDO|null
	 */
	protected ?PDO $pdo = null;
	
	/**
	 * Constructor
	 *
	 * @param string $name The name of the instance
	 * @param array $config Instance config array
	 */
	public function __construct(string $name, array $config) {
		$this->config = $config + static::getDefaults();
		static::registerInstance($name, $this);
	}
	
	public function ensureConnection(): void {
		if( !$this->isConnected() ) {
			$this->connect();
		}
	}
	
	public function isConnected(): bool {
		return !!$this->pdo;
	}
	
	protected function executeRawQuery(string $query, int $fetch) {
		try {
			$this->ensureConnection();
			if( matchBits($fetch, self::PROCESS_EXEC) ) {
				// Execute query (insert, update, delete)
				return $this->pdo->exec($query);
			}
			// Query (select, expect results)
			$pdoStatement = $this->pdo->query($query);
			$returnValue = null;
			if( matchBits($fetch, self::QUERY_STATEMENT) ) {
				// Get statement only
				return $pdoStatement;
				
			} else if( matchBits($fetch, self::QUERY_FETCH_ALL) ) {
				// Fetch all rows
				if( matchBits($fetch, self::FETCH_FIRST_COLUMN) ) {
					$returnValue = $pdoStatement->fetchAll(PDO::FETCH_ASSOC | PDO::FETCH_COLUMN, 0);
				} else {
					$returnValue = $pdoStatement->fetchAll(PDO::FETCH_ASSOC);
				}
				
			} else if( matchBits($fetch, self::QUERY_FETCH_ONE) ) {
				// Fetch first row only
				if( matchBits($fetch, self::FETCH_FIRST_COLUMN) ) {
					$returnValue = $pdoStatement->fetchColumn();
				} else {
					$returnValue = $pdoStatement->fetch(PDO::FETCH_ASSOC);
				}
				$pdoStatement->fetchAll();
			}
			$pdoStatement->closeCursor();
			unset($pdoStatement);
			
			return $returnValue;
		} catch( PDOException $exception ) {
			$this->processPdoException($exception);
		}
	}
	
	protected function processPdoException(PDOException $exception, ?string $query = null): void {
		throw new SqlException('PDO ERROR : ' . $exception->getMessage(), 'Query: ' . $query, $exception);
	}
	
	/**
	 * @warning Unsupported by MyISAM which is the default and only supported engine for now
	 */
	public function startTransaction(): AbstractSqlAdapter {
		$this->ensureConnection();
		$this->pdo->beginTransaction();
		
		return $this;
	}
	
	public function endTransaction(): AbstractSqlAdapter {
		$this->pdo->commit();
		
		return $this;
	}
	
	public function revertTransaction(): AbstractSqlAdapter {
		$this->pdo->rollBack();
		
		return $this;
	}
	
	/**
	 * Connect to the DBMS
	 */
	protected abstract function connect(): void;
	
	/**
	 * Get defaults configuration to fill missing options
	 */
	protected static function getDefaults(): array {
		return [
			'host'   => '127.0.0.1',
			'user'   => 'root',
			'passwd' => '',
		];
	}
	
	/**
	 * Register a unique instance by its name
	 */
	protected static function registerInstance(string $name, AbstractSqlAdapter $adapter): void {
		static::$instances[$name] = $adapter;
	}
	
	/**
	 * Query the DB server
	 *
	 * @param string $query The query to execute.
	 * @param int $fetch See PDO constants above. Optional, default is PROCESS_QUERY.
	 * @return mixed The result of pdo_query()
	 * @see pdo_query()
	 */
	public function query(string $query, int $fetch = self::PROCESS_QUERY): mixed {
		return $this->executeRawQuery($query, $fetch);
	}
	
	/**
	 * Select something from database
	 *
	 * @param array $options The options used to build the query
	 * @return mixed Mixed return, depending on the 'output' option
	 */
	public abstract function select(array $options = []): mixed;
	
	/**
	 * Update something in database
	 *
	 * @param array $options The options used to build the query
	 * @return int The number of affected rows
	 */
	public abstract function update(array $options = []): mixed;
	
	/**
	 * Insert something in database
	 *
	 * @param array $options The options used to build the query
	 * @return int The number of inserted rows
	 */
	public abstract function insert(array $options = []): mixed;
	
	/**
	 * Delete something in database
	 *
	 * @param array $options The options used to build the query
	 * @return int The number of deleted rows
	 */
	public abstract function delete(array $options = []): mixed;
	
	/**
	 * Get the last inserted ID
	 * It requires a successful call of insert() !
	 *
	 * @param string $table The table to get the last inserted id
	 * @return string|null The last inserted id value
	 * @noinspection PhpUnusedParameterInspection
	 */
	public function lastId(string $table): ?string {
		return $this->pdo->lastInsertId() ?: null;
	}
	
	/**
	 * Format a list of values
	 */
	public function formatValueList(array $list): string {
		$string = '';
		foreach( $list as $i => $v ) {
			$string .= ($i ? ',' : '') . $this->formatValue($v);
		}
		
		return $string;
	}
	
	/**
	 * Format SQL value
	 *
	 * @param mixed $value The value to format.
	 * @return string The formatted value.
	 *
	 * Format the given value to the matching SQL type.
	 * If the value is a float, we make french decimal compatible with SQL.
	 * If null, we use the NULL value, else we consider it as a string value.
	 */
	public function formatValue(mixed $value): string {
		return $this->escapeValue($value);
	}
	
	/**
	 * Escape the given value to the matching SQL type.
	 * If the value is a float, we make french decimal compatible with SQL.
	 * If null, we use the NULL value, else we consider it as a string value.
	 *
	 * @param mixed $value The value to format
	 * @return string The formatted value
	 */
	public function escapeValue(mixed $value): string {
		if( is_bool($value) ) {
			return $value ? 1 : 0;
		}
		if( is_object($value) && method_exists($value, 'id') ) {
			return $value->id();
		}
		
		return $value === null ? 'NULL' : $this->formatString($value);
	}
	
	/**
	 * Format the given string as an SQL string.
	 *
	 * @param string $string The string to format
	 * @return string The formatted string
	 */
	public function formatString(string $string): string {
		return "'" . str_replace("'", "''", $string) . "'";
	}
	
	/**
	 * Set the id field
	 *
	 * @param string $field The new ID field.
	 * @return static
	 */
	public function setDefaultIdField(string $field): AbstractSqlAdapter {
		$this->defaultIdField = $field;
		
		return $this;
	}
	
	public function getPdo(): ?PDO {
		return $this->pdo;
	}
	
	/**
	 * Format the given $fields into an escaped SQL string list of key=value
	 */
	protected function formatFieldList(array|string $fields): string {
		if( is_string($fields) ) {
			// Field list as string
			return $fields;
		}
		// Field list as array
		$string = '';
		foreach( $fields as $key => $value ) {
			$string .= ($string ? ', ' : '') . $this->escapeIdentifier($key) . '=' . $this->formatValue($value);
		}
		
		return $string;
	}
	
	/**
	 * Escapes the given string as an SQL identifier.
	 *
	 * @param string $identifier The identifier to escape
	 * @return string The escaped identifier
	 */
	public function escapeIdentifier(string $identifier): string {
		return '"' . $identifier . '"';
	}
	
	public function getConfig(): array {
		return $this->config;
	}
	
	/**
	 * List all instance's configuration
	 */
	public static function listConfig(): array {
		if( static::$configs === null ) {
			$cache = new ApcCache('SqlAdapter', 'db_configs', 2 * 3600);
			if( !$cache->get($configs) ) {
				$fileConfig = IniConfig::build(self::CONFIG, true, false)->all;
				$configs = [];
				foreach( $fileConfig as $key => $value ) {
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
			static::$configs = $configs;
		}
		
		return static::$configs;
	}
	
	/**
	 * Register a driver adapter
	 */
	public static function registerAdapter(string $driver, string $class): void {
		static::$adapters[$driver] = $class;
	}
	
	/**
	 * Prepare the query for the given instance
	 *
	 * @param array $options The options used to build the query.
	 * @param string|null $instance The db instance used to send the query.
	 * @param String|null $idField The ID field of the table.
	 */
	public static function prepareQuery(array &$options = [], ?string &$instance = null, ?string $idField = null): void {
		if( $idField ) {
			$adapter = static::getInstance($instance);
			$adapter->setDefaultIdField($idField);
		}
		if( !empty($options) && !empty($options['output']) && $options['output'] == AbstractSqlAdapter::RETURN_ARRAY_FIRST ) {
			$options['number'] = 1;
		}
	}
	
	public static function getInstance(?string &$instance = null): AbstractSqlAdapter {
		if( !$instance ) {
			$instance = self::DEFAULT_INSTANCE;
		}
		if( !isset(self::$instances[$instance]) ) {
			self::$instances[$instance] = self::make($instance);
		}
		
		return self::$instances[$instance];
	}
	
	/**
	 * Try to make a AbstractSqlAdapter by its name loading from configuration.
	 *
	 * @param string|null $name
	 */
	public static function make(string $name = null): AbstractSqlAdapter {
		$configs = static::listConfig();
		if( !$name ) {
			$name = self::DEFAULT_INSTANCE;
		}
		
		if( !isset($configs[$name]) ) {
			throw new SqlException(sprintf('Database configuration with name "%s" not found.', $name), 'Loading configuration');
		}
		
		$config = $configs[$name];
		
		if( empty($config['driver']) ) {
			throw new SqlException(sprintf('Database configuration with name "%s" has no driver property.', $name), 'Loading configuration');
		}
		
		if( empty(static::$adapters[$config['driver']]) ) {
			throw new SqlException(sprintf('Database configuration with name "%s" requires an unknown driver "%s".', $name, $config['driver']), 'Loading configuration');
		}
		
		/** @var class-string<AbstractSqlAdapter> $adapterClass */
		$adapterClass = static::$adapters[$config['driver']];
		
		return new $adapterClass($name, $config);
		
	}
	
	/**
	 * Get the driven string
	 */
	public abstract static function getDriver(): string;
	
}
