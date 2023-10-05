<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\SqlRequest\Facet;

/**
 * @method get(string $string, mixed $array = null)
 * @method set(string $string, mixed $where)
 * @method sget(string $string, string|null $fields)
 */
trait FieldListFacet {
	
	/**
	 * Set/Get the field list to get
	 *
	 * @param array|null $fields List of fields and values
	 * @return mixed|$this
	 */
	public function fields(?array $fields = null): mixed {
		return $this->sget('what', $fields);
	}
	
	
}
