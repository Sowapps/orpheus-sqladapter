<?php
/**
 * SqlSelectRequest
 */

namespace Orpheus\SqlRequest;

use DateTime;
use Exception;
use Iterator;
use Orpheus\Publisher\PermanentObject\PermanentObject;
use Orpheus\SqlAdapter\SqlAdapter;
use PDOStatement;

/**
 * The main SQL Select Request class
 *
 * This class handles sql SELECT request to the DBMS server.
 */
class SqlSelectRequest extends SqlRequest implements Iterator {
	
	/**
	 * Is using cache for results
	 *
	 * @var boolean
	 */
	protected bool $usingCache = true;
	
	/**
	 * The current fetch statement
	 *
	 * @var PDOStatement|null
	 */
	protected ?PDOStatement $fetchLastStatement = null;
	
	/**
	 * The current fetch is expecting an object
	 *
	 * @var boolean
	 */
	protected bool $fetchIsObject = false;
	
	/**
	 * The current index
	 *
	 * @var int
	 */
	protected int $currentIndex = -1;
	
	/**
	 * The current row
	 *
	 * @var mixed
	 */
	protected mixed $currentRow = null;
	
	/**
	 * The filter list
	 *
	 * @var callable[]
	 */
	protected array $filters = [];
	
	/**
	 * Filter results using callable, the callable must return a boolean
	 *
	 * @return $this
	 */
	public function filter(callable $filter): SqlSelectRequest {
		$this->filters[] = $filter;
		
		return $this;
	}
	
	/**
	 * Disable the class objects' cache
	 *
	 * @return $this
	 * @see setUsingCache()
	 */
	public function disableCache(): SqlSelectRequest {
		return $this->setUsingCache(false);
	}
	
	/**
	 * Set the class objects is using cache when getting results
	 *
	 * @param boolean $usingCache
	 * @return $this
	 */
	public function setUsingCache(bool $usingCache): SqlSelectRequest {
		$this->usingCache = $usingCache;
		
		return $this;
	}
	
	/**
	 * Set/Get the field list to get
	 *
	 * @param string|string[] $fields
	 * @return mixed|$this
	 */
	public function fields($fields = null): mixed {
		return $this->sget('what', $fields);
	}
	
	/**
	 * Add a field to the field list
	 *
	 * @param string $field
	 * @return $this
	 *
	 * The current field list must be a string
	 */
	public function addField(string $field): static {
		return $this->set('what', $this->get('what', '*') . ',' . $field);
	}
	
	/**
	 * Set/Get the having condition
	 *
	 * @param string|array|null $condition
	 * @return array|static
	 */
	public function having($condition = null): array|static {
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
	 * @param string|array $condition
	 * @param string $operator
	 * @param string $value
	 * @param bool $escapeValue
	 * @return static
	 */
	public function where($condition, $operator = null, $value = null, $escapeValue = true): static {
		$where = $this->get('where', []);
		$where[] = $this->formatCondition($condition, $operator, $value, $escapeValue);
		
		return $this->set('where', $where);
	}
	
	/**
	 * @param string|array $condition The condition or the field
	 * @param string|mixed|null $operator Value or operator
	 * @param mixed|null $value The value to use
	 * @param bool $escapeValue Should the value be escaped ?
	 * @return string
	 */
	public function formatCondition($condition, $operator, $value, bool $escapeValue = true): string {
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
	 * @param string|null $fields
	 * @return mixed|SqlSelectRequest
	 */
	public function orderBy(?string $fields = null): mixed {
		return $this->sget('orderby', $fields);
	}
	
	/**
	 * Set/Get the group by filter
	 *
	 * @param string|null $field
	 * @return mixed|SqlSelectRequest
	 */
	public function groupBy(?string $field = null): mixed {
		return $this->sget('groupby', $field);
	}
	
	/**
	 * Set/Get the number of expected result (as limit)
	 *
	 * @param int|null $number
	 * @return mixed|SqlSelectRequest
	 */
	public function number(?int $number = null): mixed {
		return $this->maxRows($number);
	}
	
	/**
	 * Set/Get the number of expected result (as limit)
	 *
	 * @param int|null $number
	 * @return mixed|SqlSelectRequest
	 */
	public function maxRows(?int $number = null): mixed {
		return $this->sget('number', $number);
	}
	
	/**
	 * Set/Get the offset from which we are getting results
	 *
	 * @param int|null $offset
	 * @return mixed|$this
	 */
	public function fromOffset(?int $offset = null): mixed {
		return $this->sget('offset', $offset);
	}
	
	/**
	 * Add a join condition to this query
	 *
	 * @param PermanentObject|string $entity
	 * @param string|null $alias
	 * @param string|null $byMyField
	 * @param string|null $byTheirField
	 * @param bool $mandatory
	 * @return $this
	 * @throws Exception
	 */
	public function join(PermanentObject|string $entity, ?string &$alias = null, ?string $byMyField = null, ?string $byTheirField = null, bool $mandatory = false): static {
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
		
		return $this->set('join', $joins);
	}
	
	/**
	 * Get the name to use in SQL Query
	 *
	 * @return string
	 */
	public function getEntityName(): string {
		return $this->get('alias') ?: $this->get('table');
	}
	
	/**
	 * @param string|null $alias
	 * @return mixed|$this
	 */
	public function alias(?string $alias = null): mixed {
		return $this->sget('alias', $alias);
	}
	
	/**
	 * @param string $alias
	 * @param boolean $defaultOnly
	 * @return string
	 */
	public function setAlias(string $alias, $defaultOnly = false): string {
		if( $defaultOnly ) {
			// Set only if undefined
			$value = $this->get('alias');
			if( $value === null ) {
				$this->set('alias', $alias);
			} else {
				$alias = $value;
			}
		} else {
			// Force setting alias
			$this->set('alias', $alias);
		}
		
		return $alias;
	}
	
	/**
	 * @param bool|null $isDistinct
	 * @return mixed|$this
	 */
	public function distinct(?bool $isDistinct = null): mixed {
		return $this->sget('distinct', $isDistinct);
	}
	
	/**
	 * Set the output to be an object
	 *
	 * @return $this
	 */
	public function asObject(): SqlSelectRequest {
		return $this->output(SqlAdapter::OBJECT);
	}
	
	/**
	 * Set the output to be a list of object
	 *
	 * @return $this
	 */
	public function asObjectList(): SqlSelectRequest {
		return $this->output(SqlAdapter::ARR_OBJECTS);
	}
	
	/**
	 * Set the output to be a list of array
	 *
	 * @return $this
	 */
	public function asArrayList(): SqlSelectRequest {
		return $this->output(SqlAdapter::ARR_ASSOC);
	}
	
	/**
	 * Test if the query has any result
	 *
	 * @return boolean
	 * @throws Exception
	 */
	public function exists(): bool {
		return !!$this->count(1);
	}
	
	/**
	 * Count the number of result of this query
	 *
	 * @param int $max The max number where are expecting
	 * @return int
	 * @throws Exception
	 */
	public function count($max = ''): int {
		$countKey = '0rpHeus_Count';
		$query = $this->getClone(false);
		
		$result = $query->set('what', 'COUNT(*) ' . $countKey)
			->from('(' . $this->getQuery() . ') oq')
			->asArray()
			->run();
		
		return $result[$countKey] ?? 0;
	}
	
	/**
	 * Set the output to be an array
	 *
	 * @return $this
	 */
	public function asArray(): SqlSelectRequest {
		return $this->output(SqlAdapter::ARR_FIRST);
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::current()
	 */
	public function current(): mixed {
		return $this->currentRow;
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::key()
	 */
	public function key(): mixed {
		return $this->currentIndex;
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::valid()
	 */
	public function valid(): bool {
		return $this->fetchLastStatement !== null && $this->currentRow !== null;
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::rewind()
	 */
	public function rewind(): void {
		$this->currentIndex = -1;
		$this->currentRow = null;
		$this->next();
	}
	
	/**
	 * {@inheritDoc}
	 * @see Iterator::next()
	 */
	public function next(): void {
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
	 * @return PermanentObject|mixed|null
	 *
	 * Query one time the DBMS and fetch result for next calls
	 * This feature is made for common used else it may have an unexpected behavior
	 */
	public function fetch(): mixed {
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
		/** @var PermanentObject $class */
		$class = $this->class;
		
		return $class::load($row, true, $this->usingCache);
	}
	
	/**
	 * {@inheritDoc}
	 * @see SqlRequest::run()
	 */
	public function run() {
		$options = $this->parameters;
		$onlyOne = $objects = 0;
		if( in_array($options['output'], [SqlAdapter::ARR_OBJECTS, SqlAdapter::OBJECT]) ) {
			if( $options['output'] == SqlAdapter::OBJECT ) {
				$options['number'] = 1;
				$onlyOne = 1;
			}
			$options['output'] = SqlAdapter::ARR_ASSOC;
			$objects = 1;
		}
		$options['idField'] = $this->getIDField();
		$r = $this->sqlAdapter->select($options);
		if( is_object($r) ) {
			return $r;
		}
		if( !$r && in_array($options['output'], [SqlAdapter::ARR_ASSOC, SqlAdapter::ARR_OBJECTS, SqlAdapter::ARR_FIRST]) ) {
			return $onlyOne && $objects ? null : [];
		}
		/** @var PermanentObject $class */
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
	public function isRowPassingFilters($row): bool {
		foreach( $this->filters as $filter ) {
			if( !call_user_func($filter, $row) ) {
				return false;
			}
		}
		
		return true;
	}
	
	protected function startFetching(): void {
		$this->fetchIsObject = $this->get('output', SqlAdapter::ARR_OBJECTS) === SqlAdapter::ARR_OBJECTS;
		$this->set('output', SqlAdapter::STATEMENT);
		$this->fetchLastStatement = $this->run();
	}
	
}
