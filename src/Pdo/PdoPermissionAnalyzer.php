<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\Pdo;

use Orpheus\SqlAdapter\Exception\SqlException;
use Orpheus\SqlAdapter\AbstractSqlAdapter;

abstract class PdoPermissionAnalyzer {
	
	public abstract function canDatabaseCreate();
	
	public static function fromSqlAdapter(AbstractSqlAdapter $sqlAdapter): PdoPermissionAnalyzer {
		return match ($sqlAdapter::getDriver()) {
			'mysql' => PdoMySqlPermissionAnalyzer::fromSqlAdapter($sqlAdapter),
			default => throw new SqlException(sprintf('Unknown driver %s', $sqlAdapter::getDriver()), 'Analyzing permissions'),
		};
	}
	
}
