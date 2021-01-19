<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\Pdo;

abstract class PdoPermissionAnalyzer {
	
	public abstract function canDatabaseCreate();
	
	/**
	 * @param array $settings
	 * @return PdoMySqlPermissionAnalyzer
	 */
	public static function fromSettings(array $settings) {
		switch( $settings['driver'] ) {
			case 'mysql':
				return PdoMySqlPermissionAnalyzer::fromSettings($settings);
				break;
		}
		throw new Exception(sprintf('Unknown driver %s', $settings['driver']));
	}
	
}
