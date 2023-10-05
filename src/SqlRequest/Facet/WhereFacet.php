<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\SqlRequest\Facet;

use Orpheus\SqlRequest\SqlSelectRequest;

/**
 * @method get(string $string, mixed $array = null)
 * @method formatCondition(array|string $condition, string|null $operator, mixed $value, bool $escapeValue)
 * @method set(string $string, mixed $where)
 * @method sget(string $string, string|null $fields)
 */
trait WhereFacet {
	
	/**
	 * Set the where clause
	 *
	 * If only $condition is provided, this is used as complete string, e.g. where("id = 5")
	 * If $equality & $value are provided, it uses it with $condition as a field (identifier), e.g. where('id', '=', '5')
	 * where identifier and value are escaped with escapeIdentifier() & escapeValue()
	 * If $equality is provided but $value is not, $equality is the value and where are using a smart comparator, e.g. where('id', '5')
	 * All examples return the same results. Smart comparator is IN for array values and = for all others.
	 */
	public function where(array|string $condition, ?string $operator = null, ?string $value = null, bool $escapeValue = true): static {
		$where = $this->get('where', []);
		$where[] = $this->formatCondition($condition, $operator, $value, $escapeValue);
		
		return $this->set('where', $where);
	}
	
	/**
	 * Set/Get the order by filter
	 *
	 * @return mixed|static
	 */
	public function orderBy(?string $fields = null): mixed {
		return $this->sget('orderby', $fields);
	}
	
	/**
	 * Set/Get the number of expected result (as limit)
	 *
	 * @return mixed|SqlSelectRequest
	 */
	public function number(?int $number = null): mixed {
		return $this->maxRows($number);
	}
	
	/**
	 * Set/Get the number of expected result (as limit)
	 *
	 * @return mixed|SqlSelectRequest
	 */
	public function maxRows(?int $number = null): mixed {
		return $this->sget('number', $number);
	}
	
	/**
	 * Set/Get the offset from which we are getting results
	 *
	 * @return mixed|$this
	 */
	public function fromOffset(?int $offset = null): mixed {
		return $this->sget('offset', $offset);
	}
	
}
