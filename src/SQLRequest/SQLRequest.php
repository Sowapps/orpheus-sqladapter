<?php
/**
 * SQLRequest
 */

namespace Orpheus\SQLRequest;

use Exception;
use Orpheus\SQLAdapter\SQLAdapter;

/**
 * The main SQL Request class
 *
 * This class handles sql request to the DMBS server.
 */
abstract class SQLRequest {
	
	/**
	 * The SQL Adapter
	 *
	 * @var \Orpheus\SQLAdapter\SQLAdapter
	 */
	protected $sqlAdapter;
	
	/**
	 * The ID field
	 *
	 * @var string
	 */
	protected $idField;
	
	/**
	 * The class
	 *
	 * @var string
	 */
	protected $class;
	
	/**
	 * The SQL Query Parameters
	 *
	 * @var string[]
	 */
	protected $parameters;
	
	/**
	 * Constructor
	 *
	 * @param \Orpheus\SQLAdapter\SQLAdapter $sqlAdapter
	 * @param string $idField
	 * @param string $class
	 */
	protected function __construct($sqlAdapter, $idField, $class = null) {
		$this->setSQLAdapter($sqlAdapter);
		$this->setIDField($idField);
		$this->class = $class;
	}
	
	/**
	 * Set the ID field
	 *
	 * @param string $idField
	 */
	public function setIDField($idField) {
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
	public function getQuery() {
		$output = $this->get('output');
		
		try {
			$this->set('output', SQLAdapter::SQLQUERY);
			$result = $this->run();
		} catch( Exception $e ) {
		
		}
		$this->set('output', $output);
		if( isset($e) ) {
			throw $e;
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
	protected function get($parameter, $default = null) {
		return isset($this->parameters[$parameter]) ? $this->parameters[$parameter] : $default;
	}
	
	/**
	 * Set a parameter for this query
	 *
	 * @param string $parameter
	 * @param mixed $value
	 * @return $this
	 */
	protected function set($parameter, $value) {
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
	 * @return \Orpheus\SQLAdapter\SQLAdapter
	 */
	public function getSQLAdapter() {
		return $this->sqlAdapter;
	}
	
	/**
	 * Set the SQL Adapter
	 *
	 * @param \Orpheus\SQLAdapter\SQLAdapter $sqlAdapter
	 */
	public function setSQLAdapter(SQLAdapter $sqlAdapter) {
		$this->sqlAdapter = $sqlAdapter;
	}
	
	/**
	 * get the ID field
	 *
	 * @return string
	 */
	public function getIDField() {
		return $this->idField;
	}
	
	/**
	 * Set/Get the table parameter
	 *
	 * @param string $table
	 * @return mixed|\Orpheus\SQLRequest\SQLRequest
	 */
	public function from($table = null) {
		return $this->sget('table', $table);
	}
	
	/**
	 * Set/Get a parameter for this query
	 *
	 * @param string $parameter
	 * @param mixed $value
	 * @return mixed
	 *
	 * If there is a value (non-null), we set it or we get it
	 */
	protected function sget($parameter, $value = null) {
		return $value === null ? $this->get($parameter) : $this->set($parameter, $value);
	}
	
	/**
	 * Set/Get the ouput parameter
	 *
	 * @param string $output
	 * @return mixed|\Orpheus\SQLRequest\SQLRequest
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
	public function escapeIdentifier($identifier) {
		return $this->sqlAdapter->escapeIdentifier($identifier);
	}
	
	/**
	 * Escape an SQL value using SQL Adapter
	 *
	 * @param string $value
	 * @return string
	 */
	public function escapeValue($value) {
		return $this->sqlAdapter->escapeValue($value);
	}
	
	/**
	 * Create a select request
	 *
	 * @param \Orpheus\SQLAdapter\SQLAdapter $sqlAdapter
	 * @param string $idField The ID field
	 * @param string $class The class used to instanciate entries
	 * @return SQLSelectRequest
	 */
	public static function select($sqlAdapter = null, $idField = 'id', $class = null) {
		return new SQLSelectRequest($sqlAdapter, $idField, $class);
	}
}
