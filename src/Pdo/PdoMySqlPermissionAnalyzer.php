<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\Pdo;

use PDO;

class PdoMySqlPermissionAnalyzer extends PdoPermissionAnalyzer {
	
	const OBJECT_ALL = '*';
	
	const LEVEL_GLOBAL = 'global';
	const LEVEL_DATABASE = 'database';
	const LEVEL_TABLE = 'table';
	
	const RIGHT_CREATE = 'create';
	
	/** @var array */
	protected static $knownRights = [
		'CREATE' => self::RIGHT_CREATE,
	];
	
	/** @var array */
	protected $permissions;
	
	/**
	 * PdoMySqlPermissionAnalyzer constructor
	 *
	 * @param array $permissions
	 */
	public function __construct(array $permissions) {
		$this->permissions = $permissions;
	}
	
	public function canDatabaseCreate() {
		return $this->hasPermission(self::LEVEL_GLOBAL, null, self::RIGHT_CREATE);
	}
	
	/**
	 * @param $level
	 * @param $object
	 * @param $right
	 */
	public function hasPermission($level, $object, $right) {
		
	}
	
	/**
	 * @param object $settings
	 * @return PdoMySqlPermissionAnalyzer|static
	 */
	public static function fromSettings(array $settings) {
		$pdo = pdo_connect($settings, false);
		$statement = $pdo->query('SHOW GRANTS FOR CURRENT_USER;', PDO::FETCH_NUM);
		$permissions = [];
		while( $row = $statement->fetch() ) {
			$permissions[] = self::parseGrant($row[0]);
		}
		
		return new static($permissions);
	}
	
	public static function parseGrant($grant) {
		if( !preg_match('#GRANT (.+) ON `?([^`]+)`?\.`?(.+)`? TO .+#i', $grant, $values) ) {
			throw new Exception('Unable to parse MySql grant');
		}
		
		return (object) [
			'rights'   => $values[1] !== 'ALL PRIVILEGES' ? explode(', ', $values[1]) : null,
			'database' => $values[2] !== '*' ? $values[2] : null,
			'table'    => $values[3] !== '*' ? $values[3] : null,
		];
	}
	
}
