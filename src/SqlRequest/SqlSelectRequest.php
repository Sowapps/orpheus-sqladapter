<?php
/**
 * SqlSelectRequest
 */

namespace Orpheus\SqlRequest;

use Exception;
use Iterator;
use Orpheus\EntityDescriptor\Entity\PermanentEntity;
use Orpheus\SqlAdapter\AbstractSqlAdapter;
use Orpheus\SqlRequest\Facet\FieldListFacet;
use Orpheus\SqlRequest\Facet\WhereFacet;
use PDO;
use PDOStatement;

/**
 * The main SQL Select Request class
 *
 * This class handles sql SELECT request to the DBMS server.
 */
class SqlSelectRequest extends AbstractSqlRequest implements Iterator {
	
	use FieldListFacet, WhereFacet;
	
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
	
	public function run(): mixed {
		$options = $this->parameters;
		$expectOnlyOne = $expectObject = false;
		if( in_array($options['output'], [AbstractSqlAdapter::ARR_OBJECTS, AbstractSqlAdapter::RETURN_OBJECT]) ) {
			if( $options['output'] == AbstractSqlAdapter::RETURN_OBJECT ) {
				$options['number'] = 1;
				$expectOnlyOne = true;
			}
			$options['output'] = AbstractSqlAdapter::RETURN_ARRAY_ASSOC;
			$expectObject = true;
		}
		$options['idField'] = $this->getIdField();
		$result = $this->sqlAdapter->select($options);
		if( is_object($result) ) {
			return $result;
		}
		if( !$result && in_array($options['output'], [AbstractSqlAdapter::RETURN_ARRAY_ASSOC, AbstractSqlAdapter::ARR_OBJECTS, AbstractSqlAdapter::RETURN_ARRAY_FIRST]) ) {
			return $expectOnlyOne && $expectObject ? null : [];
		}
		if( $result && is_array($result) && $expectObject ) {
			if( $expectOnlyOne ) {
				$result = $this->formatObject($result[0]);
				if( !$result || !$this->isRowPassingFilters($result) ) {
					$result = null;
				}
			} else {
				$results = [];
				foreach( $result as $row ) {
					$entity = $this->formatObject($row);
					// Why ? if( !$entity || $this->isRowPassingFilters($entity) ) {
					if( $entity && $this->isRowPassingFilters($entity) ) {
						$results[] = $entity;
					}
				}
				$result = $results;
			}
		}
		
		return $result;
	}
	
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
	 * @return $this
	 */
	public function setUsingCache(bool $usingCache): SqlSelectRequest {
		$this->usingCache = $usingCache;
		
		return $this;
	}
	
	/**
	 * Add a field to the field list
	 *
	 * @return $this
	 *
	 * The current field list must be a string
	 */
	public function addField(string $field): static {
		return $this->set('what', $this->get('what', '*') . ',' . $field);
	}
	
	/**
	 * Set/Get the having condition
	 */
	public function having(array|string|null $condition = null): array|static {
		$having = $this->get('having', []);
		if( !$condition ) {
			return $having;
		}
		$having[] = $condition;
		
		return $this->set('having', $having);
	}
	
	/**
	 * Set/Get the group by filter
	 *
	 * @return mixed|SqlSelectRequest
	 */
	public function groupBy(?string $field = null): mixed {
		return $this->sget('groupby', $field);
	}
	
	/**
	 * Add a join condition to this query
	 *
	 * @return $this
	 * @throws Exception
	 */
	public function join(string $entityClass, ?string &$alias = null, ?string $byMyField = null, ?string $byTheirField = null, bool $mandatory = false): static {
		$joins = $this->get('join', []);
		if( !$byMyField && !$byTheirField ) {
			throw new Exception('Unable to generate smart join with both missing $byMyField & $byTheirField parameters');
		}
		if( !class_exists($entityClass) || !is_subclass_of($entityClass, PermanentEntity::class) ) {
			throw new Exception('Entity must be a valid Entity Class extending PermanentEntity');
		}
		if( !$byMyField ) {
			$byMyField = $this->idField;
		}
		/** @var PermanentEntity $entityClass */
		if( !$byTheirField ) {
			$byTheirField = $entityClass::getIdField();
			if( !$alias ) {
				$number = count($joins) + 1;
				$alias = 'j' . $number;
			}
			$joins[] = (object) [
				'table' => $entityClass::getTable(),
				'alias'     => $alias,
				'condition' => sprintf('%s = %s', $this->escapeIdentifier($alias . '.' . $byTheirField), $this->escapeIdentifier($this->getEntityName() . '.' . $byMyField)),
				'mandatory' => $mandatory,
			];
		}
		
		return $this->set('join', $joins);
	}
	
	/**
	 * Get the name to use in SQL Query
	 */
	public function getEntityName(): string {
		return $this->get('alias') ?: $this->get('table');
	}
	
	/**
	 * @return mixed|$this
	 */
	public function alias(?string $alias = null): mixed {
		return $this->sget('alias', $alias);
	}
	
	public function setAlias(string $alias, bool $defaultOnly = false): string {
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
		return $this->output(AbstractSqlAdapter::RETURN_OBJECT);
	}
	
	/**
	 * Set the output to be a list of object
	 *
	 * @return $this
	 */
	public function asObjectList(): SqlSelectRequest {
		return $this->output(AbstractSqlAdapter::ARR_OBJECTS);
	}
	
	/**
	 * Set the output to be a list of array
	 *
	 * @return $this
	 */
	public function asArrayList(): SqlSelectRequest {
		return $this->output(AbstractSqlAdapter::RETURN_ARRAY_ASSOC);
	}
	
	/**
	 * Test if the query has any result
	 *
	 * @throws Exception
	 */
	public function exists(): bool {
		return !!$this->count(1);
	}
	
	/**
	 * Count the number of result of this query
	 */
	public function count(?int $max = null): int {
		$countKey = '0rpHeus_Count';
		$query = $this->getClone(false);
		
		$query->set('what', 'COUNT(*) ' . $countKey)
			->from('(' . $this->getQuery() . ') oq')
			->asArray();
		if( $max ) {
			// Exists case, we don't need to count more than one
			$query->number($max);
		}
		$query->run();
		
		return $result[$countKey] ?? 0;
	}
	
	/**
	 * Set the output to be an array
	 *
	 * @return $this
	 */
	public function asArray(): SqlSelectRequest {
		return $this->output(AbstractSqlAdapter::RETURN_ARRAY_FIRST);
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
	public function key(): int {
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
			$row = $this->fetch();
			// Pass the filtered rows
		} while( $row && !$this->isRowPassingFilters($row) );
		// The index is not filtered
		$this->currentRow = $row;
		$this->currentIndex++;
	}
	
	/**
	 * Fetch the next result of this query
	 *
	 * @return PermanentEntity|mixed|null
	 *
	 * Query one time the DBMS and fetch result for next calls
	 * This feature is made for common used else it may have an unexpected behavior
	 */
	public function fetch(): mixed {
		if( !$this->fetchLastStatement ) {
			$this->startFetching();
		}
		$row = $this->fetchLastStatement->fetch(PDO::FETCH_ASSOC);
		if( !$row ) {
			// Last return false, we return null, same effect
			return null;
		}
		if( $this->fetchIsObject ) {
			return $this->formatObject($row);
		}
		
		return $row;
	}
	
	protected function formatObject(array $row): ?PermanentEntity {
		/** @var class-string<PermanentEntity> $class */
		$class = $this->class;
		
		return $class::buildRaw($row, $this->usingCache);
	}
	
	/**
	 * @param PermanentEntity|array|scalar $row
	 */
	public function isRowPassingFilters(mixed $row): bool {
		foreach( $this->filters as $filter ) {
			if( !call_user_func($filter, $row) ) {
				return false;
			}
		}
		
		return true;
	}
	
	protected function startFetching(): void {
		$this->fetchIsObject = intval($this->get('output', AbstractSqlAdapter::ARR_OBJECTS)) === AbstractSqlAdapter::ARR_OBJECTS;
		$this->set('output', AbstractSqlAdapter::STATEMENT);
		$this->fetchLastStatement = $this->run();
	}
	
}
