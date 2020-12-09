<?php
/**
 * SQLSelectRequest
 */

namespace Orpheus\SQLRequest;

use DateTime;
use Exception;
use Iterator;
use Orpheus\SQLAdapter\SQLAdapter;
use PDOStatement;

/**
 * The main SQL Select Request class
 *
 * This class handles sql SELECT request to the DMBS server.
 */
class SQLSelectRequest extends SQLRequest implements Iterator {
	
	/**
	 * Is using cache for results
	 *
	 * @var boolean
	 */
	protected $usingCache = true;
	
	/**
	 * The current fetch statement
	 *
	 * @var PDOStatement
	 */
	protected $fetchLastStatement;
	
	/**
	 * The current fetch is expecting an object
	 *
	 * @var boolean
	 */
	protected $fetchIsObject;
	
	/**
	 * The current index
	 *
	 * @var int
	 */
	protected $currentIndex;
	
	/**
	 * The current row
	 *
	 * @var mixed
	 */
	protected $currentRow;
	
	/**
	 * The filter list
	 *
	 * @var callable[]
	 */
	protected $filters = [];
	
	/**
	 * Filter results using callable, the callable must return a boolean
	 *
	 * @return $this
	 */
	public function filter(callable $filter) {
		$this->filters[] = $filter;
		return $this;
	}
	
	/**
	 * Disable the class objects' cache
	 *
	 * @return $this
	 * @see setUsingCache()
	 */
	public function disableCache() {
		return $this->setUsingCache(false);
	}
	
	/**
	 * Set the class objects is using cache when getting results
	 *
	 * @param boolean $usingCache
	 * @return \Orpheus\SQLRequest\SQLSelectRequest
	 */
	public function setUsingCache($usingCache) {
		$this->usingCache = $usingCache;
		return $this;
	}
	
	/**
	 * Set/Get the field list to get
	 *
	 * @param string|string[] $fields
	 * @return mixed|$this
	 */
	public function fields($fields = null) {
		return $this->sget('what', $fields);
	}
	
	/**
	 * Add a field to to the field list
	 *
	 * @param string $field
	 * @return $this
	 *
	 * The current field list must be a string
	 */
	public function addField($field) {
		return $this->sget('what', $this->get('what', '*') . ',' . $field);
	}
	
	/**
	 * Set/Get the having condition
	 *
	 * @param string $condition
	 * @return array|static
	 */
	public function having($condition = null) {
		$having = $this->get('having', []);
		if( !$condition ) {
			return $having;
		}
		$having[] = $condition;
		return $this->set('having', $having);
	}
	
	/**
	 * Set the where clause
	 *
	 * If only $condition is provided, this is used as complete string, e.g where("id = 5")
	 * If $equality & $value are provided, it uses it with $condition as a field (identifier), e.g where('id', '=', '5')
	 * where identifier and value are escaped with escapeIdentifier() & escapeValue()
	 * If $equality is provided but $value is not, $equality is the value and where are using a smart comparator, e.g where('id', '5')
	 * All examples return the same results. Smart comparator is IN for array values and = for all other.
	 *
	 * @param array|string $condition
	 * @param string $operator
	 * @param string $value
	 * @param bool $escapeValue
	 * @return static
	 */
	public function where($condition, $operator = null, $value = null, $escapeValue = true) {
		$where = $this->get('where', []);
		$where[] = $this->formatCondition($condition, $operator, $value, $escapeValue);
		return $this->sget('where', $where);
	}
	
	/**
	 * @param string $condition The condition or the field
	 * @param string|mixed|null $operator Value or operator
	 * @param mixed|null $value The value to use
	 * @param bool $escapeValue Should the value be escaped ?
	 * @return string
	 */
	public function formatCondition($condition, $operator, $value, $escapeValue = true) {
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
				return $this->formatCondition($condition[0],
					isset($condition[1]) ? $condition[1] : null,
					isset($condition[2]) ? $condition[2] : null,
					isset($condition[3]) ? $condition[3] : true
				);
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
					
				} elseif( is_object($value) ) {
					// Value is PermanentObject
					// Id Could be a string, so we have to escape it too
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
	 * Set/Get the order by filter
	 *
	 * @param string $fields
	 * @return mixed|\Orpheus\SQLRequest\SQLSelectRequest
	 */
	public function orderby($fields = null) {
		return $this->sget('orderby', $fields);
	}
	
	/**
	 * Set/Get the group by filter
	 *
	 * @param string $field
	 * @return mixed|\Orpheus\SQLRequest\SQLSelectRequest
	 */
	public function groupby($field = null) {
		return $this->sget('groupby', $field);
	}
	
	/**
	 * Set/Get the number of expected result (as limit)
	 *
	 * @param int $number
	 * @return mixed|\Orpheus\SQLRequest\SQLSelectRequest
	 */
	public function number($number = null) {
		return $this->maxRows($number);
	}
	
	/**
	 * Set/Get the number of expected result (as limit)
	 *
	 * @param int $number
	 * @return mixed|\Orpheus\SQLRequest\SQLSelectRequest
	 */
	public function maxRows($number = null) {
		return $this->sget('number', $number);
	}
	
	/**
	 * Set/Get the offset from which we are getting results
	 *
	 * @param int $offset
	 * @return mixed|$this
	 */
	public function fromOffset($offset = null) {
		return $this->sget('offset', $offset);
	}
	
	/**
	 * Add a join condition to this query
	 *
	 * @param string $join
	 * @return $this
	 * @throws Exception
	 */
	public function join($entity, &$alias = null, $byMyField = null, $byTheirField = null, $mandatory = false) {
		$joins = $this->get('join', []);
		if( is_string($entity) && func_num_args() === 1 ) {
			// Raw join string
			$joins[] = $entity;
		} else {
			// Smart one
			if( !$byMyField && !$byTheirField ) {
				throw new Exception('Unable to generate smart join with both missing $byMyField & $byTheirField parameters');
			}
			$isClass = class_exists($entity);
			if( !$byMyField ) {
				$byMyField = $this->idField;
			}
			if( !$byTheirField ) {
				if( !$isClass ) {
					throw new Exception('Unable to generate smart join with missing $byTheirField parameters');
				}
				$byTheirField = $entity::getIDField();
			}
			if( !$alias ) {
				$number = count($joins) + 1;
				$alias = 'j' . $number;
			}
			$joins[] = (object) [
				'table'     => class_exists($entity) ? $entity::getTable() : $entity,
				'alias'     => $alias,
				'condition' => sprintf('%s = %s', $this->escapeIdentifier($alias . '.' . $byTheirField), $this->escapeIdentifier($this->getEntityName() . '.' . $byMyField)),
				'mandatory' => $mandatory,
			];
		}
		return $this->sget('join', $joins);
	}
	
	/**
	 * Get the name to use in SQL Query
	 */
	public function getEntityName() {
		return $this->get('alias') ?: $this->get('table');
	}
	
	/**
	 * @param string $alias
	 * @return mixed|$this
	 */
	public function alias($alias) {
		return $this->sget('alias', $alias);
	}
	
	/**
	 * @param string $alias
	 * @param boolean $defaultOnly
	 * @return string
	 */
	public function setAlias($alias, $defaultOnly = false) {
		if( $defaultOnly ) {
			// Set only if undefined
			$value = $this->get('alias');
			if( $value === null ) {
				$this->set('alias', $alias);
			} else {
				$alias = $value;
			}
		} else {
			// Force to set alias
			$this->set('alias', $alias);
		}
		return $alias;
	}
	
	/**
	 * @param bool|null $isDistinct
	 * @return mixed|$this
	 */
	public function distinct($isDistinct = null) {
		return $this->sget('distinct', $isDistinct);
	}
	
	/**
	 * Set the output to be an object
	 *
	 * @return $this
	 */
	public function asObject() {
		return $this->output(SQLAdapter::OBJECT);
	}
	
	/**
	 * Set the output to be a list of object
	 *
	 * @return $this
	 */
	public function asObjectList() {
		return $this->output(SQLAdapter::ARR_OBJECTS);
	}
	
	/**
	 * Set the output to be a list of array
	 *
	 * @return $this
	 */
	public function asArrayList() {
		return $this->output(SQLAdapter::ARR_ASSOC);
	}
	
	/**
	 * Test if the query has any result
	 *
	 * @return boolean
	 * @throws Exception
	 */
	public function exists() {
		return !!$this->count(1);
	}
	
	/**
	 * Count the number of result of this query
	 *
	 * @param int $max The max number where are expecting
	 * @return int
	 * @throws Exception
	 */
	public function count($max = '') {
		$countKey = '0rpHeus_Count';
		$query = $this->getClone(false);
		
		$result = $query->set('what', 'COUNT(*) ' . $countKey)
			->from('(' . $this->getQuery() . ') oq')
			->asArray()
			->run();
		
		return isset($result[$countKey]) ? $result[$countKey] : 0;
	}
	
	/**
	 * Set the output to be an array
	 *
	 * @return $this
	 */
	public function asArray() {
		return $this->output(SQLAdapter::ARR_FIRST);
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::current()
	 */
	public function current() {
		return $this->currentRow;
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::key()
	 */
	public function key() {
		return $this->currentIndex;
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::valid()
	 */
	public function valid() {
		return $this->fetchLastStatement != null && $this->currentRow != null;
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::rewind()
	 */
	public function rewind() {
		$this->currentIndex = -1;
		$this->currentRow = null;
		$this->next();
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::next()
	 */
	public function next() {
		do {
			$this->currentRow = $this->fetch();
			// Pass the filtered rows
		} while( $this->currentRow && !$this->isRowPassingFilters($this->currentRow) );
		// The index is not filtered
		$this->currentIndex++;
	}
	
	/**
	 * Fetch the next result of this query
	 *
	 * @return NULL|mixed
	 *
	 * Query one time the DBMS and fetch result for next calls
	 * This feature is made for common used else it may have an unexpected behavior
	 */
	public function fetch() {
		if( !$this->fetchLastStatement ) {
			$this->startFetching();
		}
		$row = $this->fetchLastStatement->fetch(\PDO::FETCH_ASSOC);
		if( !$row ) {
			// Last return false, we return null, same effect
			return null;
		}
		if( !$this->fetchIsObject ) {
			return $row;
		}
		$class = $this->class;
		return $class::load($row, true, $this->usingCache);
	}
	
	protected function startFetching() {
		$this->fetchIsObject = $this->get('output', SQLAdapter::ARR_OBJECTS) === SQLAdapter::ARR_OBJECTS;
		$this->set('output', SQLAdapter::STATEMENT);
		$this->fetchLastStatement = $this->run();
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @see \Orpheus\SQLRequest\SQLRequest::run()
	 */
	public function run() {
		$options = $this->parameters;
		$onlyOne = $objects = 0;
		if( in_array($options['output'], [SQLAdapter::ARR_OBJECTS, SQLAdapter::OBJECT]) ) {
			if( $options['output'] == SQLAdapter::OBJECT ) {
				$options['number'] = 1;
				$onlyOne = 1;
			}
			$options['output'] = SQLAdapter::ARR_ASSOC;
			$objects = 1;
		}
		$options['idField'] = $this->getIDField();
		$r = $this->sqlAdapter->select($options);
		if( is_object($r) ) {
			return $r;
		}
		if( !$r && in_array($options['output'], [SQLAdapter::ARR_ASSOC, SQLAdapter::ARR_OBJECTS, SQLAdapter::ARR_FIRST]) ) {
			return $onlyOne && $objects ? null : [];
		}
		$class = $this->class;
		if( $r && $objects ) {
			if( $onlyOne ) {
				$r = $class::load($r[0], true, $this->usingCache);
				if( !$r || !$this->isRowPassingFilters($r) ) {
					$r = null;
				}
			} else {
				$results = [];
				foreach( $r as $rdata ) {
					$row = $class::load($rdata, true, $this->usingCache);
					if( !$row || $this->isRowPassingFilters($row) ) {
						$results[] = $row;
					}
				}
				$r = $results;
			}
		}
		return $r;
	}
	
	/**
	 * @param $row
	 * @return bool
	 */
	public function isRowPassingFilters($row) {
		foreach( $this->filters as $filter ) {
			if( !call_user_func($filter, $row) ) {
				return false;
			}
		}
		return true;
	}
	
}
