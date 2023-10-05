<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\Pdo;

use Orpheus\SqlAdapter\Exception\SqlException;
use Orpheus\SqlAdapter\AbstractSqlAdapter;

class PdoMySqlPermissionAnalyzer extends PdoPermissionAnalyzer {
	
	const OBJECT_ALL = '*';
	
	const LEVEL_GLOBAL = 'global';
	const LEVEL_DATABASE = 'database';
	const LEVEL_TABLE = 'table';
	
	const RIGHT_CREATE = 'create';
	
	protected static array $knownRights = [
		'CREATE' => self::RIGHT_CREATE,
	];
	
	protected array $permissions;
	
	/**
	 * PdoMySqlPermissionAnalyzer constructor
	 */
	public function __construct(array $permissions) {
		$this->permissions = $permissions;
	}
	
	public function canDatabaseCreate(): bool {
		// self::LEVEL_GLOBAL, null, self::RIGHT_CREATE
		return $this->hasPermission();
	}
	
	public function hasPermission(): bool {
		// TODO Implement this method
		// $level, $object, $right
		return true;
	}
	
	public static function fromSqlAdapter(AbstractSqlAdapter $sqlAdapter): PdoPermissionAnalyzer {
		$statement = $sqlAdapter->query('SHOW GRANTS FOR CURRENT_USER;', AbstractSqlAdapter::QUERY_FETCH_ALL);
		// TODO Require more tests !
		$permissions = [];
		while( $row = $statement->fetch() ) {
			$permissions[] = self::parseGrant($row[0]);
		}
		
		return new static($permissions);
	}
	
	public static function parseGrant(string $grant): object {
		if( !preg_match('#GRANT (.+) ON `?([^`]+)`?\.`?(.+)`? TO .+#i', $grant, $values) ) {
			throw new SqlException('Unable to parse MySql grant', 'Parsing grant string');
		}
		
		return (object) [
			'rights'   => $values[1] !== 'ALL PRIVILEGES' ? explode(', ', $values[1]) : null,
			'database' => $values[2] !== '*' ? $values[2] : null,
			'table'    => $values[3] !== '*' ? $values[3] : null,
		];
	}
	
}
