<?php
/**
 * SqlSelectRequest
 */

namespace Orpheus\SqlRequest;

use Orpheus\SqlRequest\Facet\FieldListFacet;
use Orpheus\SqlRequest\Facet\WhereFacet;

/**
 * The main SQL Update Request class
 *
 * This class handles sql UPDATE request to the DBMS server.
 */
class SqlUpdateRequest extends AbstractSqlRequest {
	
	use FieldListFacet, WhereFacet;
	
	public function run(): string|int {
		$options = $this->parameters;
		$options['idField'] = $this->getIdField();
		
		return $this->sqlAdapter->update($options);
	}
	
}
