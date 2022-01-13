<?php
/**
 * SQLRequest
 */

namespace Orpheus\SQLRequest;

use Exception;
use Orpheus\SQLAdapter\SqlAdapter;

/**
 * The main SQL Request class
 *
 * This class handles sql request to the DBMS server.
 */
abstract class SQLRequest {
	
	/**
	 * The SQL Adapter
	 *
	 * @var SqlAdapter
	 */
	protected SqlAdapter $sqlAdapter;
	
	/**
	 * The ID field
	 *
	 * @var string
	 */
	protected string $idField;
	
	/**
	 * The class
	 *
	 * @var string|null
	 */
	protected ?string $class;
	
	/**
	 * The SQL Query Parameters
	 *
	 * @var string[]
	 */
	protected array $parameters = [];
	
	/**
	 * Constructor
	 *
	 * @param SqlAdapter $sqlAdapter
	 * @param string $idField
	 * @param string $class
	 */
	protected function __construct(SqlAdapter $sqlAdapter, string $idField, ?string $class = null) {
		$this->setSQLAdapter($sqlAdapter);
		$this->setIDField($idField);
		$this->class = $class;
	}
	
	/**
	 * Set the ID field
	 *
	 * @param string $idField
	 */
	public function setIDField(string $idField) {
		$this->idField = $idField;
	}
	
	/**
	 * Get object as string
	 *
	 * @return string
	 * @throws Exception
	 */
	public function __toString() {
		return $this->getQuery();
	}
	
	/**
	 * Get the query as string
	 *
	 * @return string
	 * @throws Exception
	 */
	public function getQuery(): string {
		// Store previous output before changing to temp
		$output = $this->get('output');
		
		try {
			$this->set('output', SqlAdapter::SQL_QUERY);
			$result = $this->run();
		} finally {
			$this->set('output', $output);
		}
		
		return $result;
	}
	
	/**
	 * Get a parameter for this query
	 *
	 * @param string $parameter
	 * @param mixed $default
	 * @return mixed
	 */
	protected function get(string $parameter, $default = null) {
		return $this->parameters[$parameter] ?? $default;
	}
	
	/**
	 * Set a parameter for this query
	 *
	 * @param string $parameter
	 * @param mixed $value
	 * @return self
	 */
	protected function set(string $parameter, $value): self {
		$this->parameters[$parameter] = $value;
		
		return $this;
	}
	
	/**
	 * Run the query and return results
	 */
	protected abstract function run();
	
	/**
	 * Get a clone of current request
	 *
	 * @param string $withParameters True to also copy parameters, default to true
	 * @return SQLRequest
	 */
	public function getClone($withParameters = true) {
		$clone = new static($this->sqlAdapter, $this->idField, $this->class);
		if( $withParameters ) {
			$clone->parameters = $this->parameters;
		}
		return $clone;
	}
	
	/**
	 * Get the SQL Adapter
	 *
	 * @return SqlAdapter
	 */
	public function getSQLAdapter(): SqlAdapter {
		return $this->sqlAdapter;
	}
	
	/**
	 * Set the SQL Adapter
	 *
	 * @param SqlAdapter $sqlAdapter
	 */
	public function setSQLAdapter(SqlAdapter $sqlAdapter) {
		$this->sqlAdapter = $sqlAdapter;
	}
	
	/**
	 * get the ID field
	 *
	 * @return string
	 */
	public function getIDField(): string {
		return $this->idField;
	}
	
	/**
	 * Set/Get the table parameter
	 *
	 * @param string $table
	 * @return self|string
	 */
	public function from($table = null) {
		return $this->sget('table', $table);
	}
	
	/**
	 * Set/Get a parameter for this query
	 * If there is a value (non-null), we set it, or we get it
	 *
	 * @param string $parameter
	 * @param mixed $value
	 * @return $this|mixed
	 */
	protected function sget($parameter, $value = null) {
		return $value === null ? $this->get($parameter) : $this->set($parameter, $value);
	}
	
	/**
	 * Set/Get the output parameter
	 *
	 * @param string $output
	 * @return $this|mixed
	 */
	public function output($output = null) {
		return $this->sget('output', $output);
	}
	
	/**
	 * Escape an SQL identifier using SQL Adapter
	 *
	 * @param string $identifier
	 * @return string
	 */
	public function escapeIdentifier($identifier): string {
		return $this->sqlAdapter->escapeIdentifier($identifier);
	}
	
	/**
	 * Escape an SQL value using SQL Adapter
	 *
	 * @param string $value
	 * @return string
	 */
	public function escapeValue($value): string {
		return $this->sqlAdapter->escapeValue($value);
	}
	
	/**
	 * Create a select request
	 *
	 * @param SqlAdapter $sqlAdapter
	 * @param string $idField The ID field
	 * @param string|null $class The class used to instantiate entries
	 * @return SQLSelectRequest
	 * @deprecated Seems not used, there are other ways to do
	 */
	public static function select(SqlAdapter $sqlAdapter, string $idField = 'id', ?string $class = null): SQLSelectRequest {
		return new SQLSelectRequest($sqlAdapter, $idField, $class);
	}
}
