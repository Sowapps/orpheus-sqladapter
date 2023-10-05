<?php
/**
 * AbstractSqlRequest
 */

namespace Orpheus\SqlRequest;

use DateTime;
use Exception;
use Orpheus\SqlAdapter\AbstractSqlAdapter;

/**
 * The main SQL Request class
 *
 * This class handles sql request to the DBMS server.
 */
abstract class AbstractSqlRequest {
	
	/**
	 * The SQL Adapter
	 *
	 * @var AbstractSqlAdapter
	 */
	protected AbstractSqlAdapter $sqlAdapter;
	
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
	 */
	public function __construct(AbstractSqlAdapter $sqlAdapter, string $idField, ?string $class = null) {
		$this->setSqlAdapter($sqlAdapter);
		$this->idField = $idField;
		$this->class = $class;
	}
	
	/**
	 * Get the name to use in SQL Query
	 */
	public function getEntityName(): string {
		return $this->from();
	}
	
	/**
	 * @param array|string $condition The condition or the field
	 * @param string|mixed|null $operator Value or operator
	 * @param mixed|null $value The value to use
	 * @param bool $escapeValue Should the value be escaped ?
	 */
	public function formatCondition(array|string $condition, mixed $operator, mixed $value, bool $escapeValue = true): string {
		if( is_array($condition) ) {
			if( $condition && is_array($condition[0]) ) {
				// Array of array => Multiple conditions, we use the OR operator
				$conditionStr = '';
				// Array of string or array of object
				foreach( $condition as $conditionRow ) {
					$conditionStr .= ($conditionStr ? ' OR ' : '') .
						$this->formatCondition($conditionRow, null, null);
					// Fall into the One condition
				}
				
				return '(' . $conditionStr . ')';
			} else {
				// One condition
				return $this->formatCondition($condition[0], $condition[1] ?? null, $condition[2] ?? null, $condition[3] ?? true);
			}
		}
		if( $operator !== null ) {
			if( $value === null ) {
				$value = $operator;
				$operator = is_array($value) ? 'IN' : '=';
			}
			if( is_array($value) ) {
				$value = '(' . $this->sqlAdapter->formatValueList($value) . ')';
			} else {
				if( $value instanceof DateTime ) {
					// Value is PHP DateTime
					$value = sqlDatetime($value);
					
				} else if( is_object($value) ) {
					// Value is PermanentEntity
					// The ID could be a string, so we have to escape it too
					$value = id($value);
				}
				if( $escapeValue ) {
					$value = $this->escapeValue($value);
				}
			}
			$condition = $this->escapeIdentifier($condition) . ' ' . $operator . ' ' . $value;
		}
		
		return $condition;
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
	 */
	public function getQuery(): string {
		// Store previous output before changing to temp
		$output = $this->get('output');
		
		try {
			$this->set('output', AbstractSqlAdapter::SQL_QUERY);
			$result = $this->run();
		} finally {
			$this->set('output', $output);
		}
		
		return $result;
	}
	
	/**
	 * Get a parameter for this query
	 */
	protected function get(string $parameter, mixed $default = null): mixed {
		return $this->parameters[$parameter] ?? $default;
	}
	
	/**
	 * Set a parameter for this query
	 */
	protected function set(string $parameter, mixed $value): static {
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
	 * @param bool $withParameters True to also copy parameters, default to true
	 */
	public function getClone(bool $withParameters = true): static {
		$clone = new static($this->sqlAdapter, $this->idField, $this->class);
		if( $withParameters ) {
			$clone->parameters = $this->parameters;
		}
		return $clone;
	}
	
	/**
	 * Get the SQL Adapter
	 */
	public function getSqlAdapter(): AbstractSqlAdapter {
		return $this->sqlAdapter;
	}
	
	/**
	 * Set the SQL Adapter
	 */
	public function setSqlAdapter(AbstractSqlAdapter $sqlAdapter): static {
		$this->sqlAdapter = $sqlAdapter;
		
		return $this;
	}
	
	/**
	 * get the ID field
	 */
	public function getIdField(): string {
		return $this->idField;
	}
	
	/**
	 * Set/Get the table parameter
	 */
	public function from(?string $table = null): static|string {
		return $this->sget('table', $table);
	}
	
	/**
	 * Set/Get a parameter for this query
	 * If there is a value (non-null), we set it, or we get it
	 *
	 * @noinspection PhpMixedReturnTypeCanBeReducedInspection
	 */
	protected function sget(string $parameter, mixed $value = null): mixed {
		return $value === null ? $this->get($parameter) : $this->set($parameter, $value);
	}
	
	/**
	 * Set/Get the output parameter
	 */
	public function output(?string $output = null): mixed {
		return $this->sget('output', $output);
	}
	
	/**
	 * Escape an SQL identifier using SQL Adapter
	 */
	public function escapeIdentifier(string $identifier): string {
		return $this->sqlAdapter->escapeIdentifier($identifier);
	}
	
	/**
	 * Escape an SQL value using SQL Adapter
	 */
	public function escapeValue(mixed $value): string {
		return $this->sqlAdapter->escapeValue($value);
	}
	
}
