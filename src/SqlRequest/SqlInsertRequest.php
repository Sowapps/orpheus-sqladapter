<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\SqlRequest;

use Orpheus\SqlRequest\Facet\FieldListFacet;

/**
 * The main SQL Insert Request class
 *
 * This class handles sql INSERT request to the DBMS server.
 */
class SqlInsertRequest extends AbstractSqlRequest {
	
	use FieldListFacet;
	
	public function run(): string|int {
		$options = $this->parameters;
		$options['idField'] = $this->getIdField();
		
		return $this->sqlAdapter->insert($options);
	}
	
	public function getLastId(): ?string {
		return $this->sqlAdapter->lastId($this->from());
	}
	
}
