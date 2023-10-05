<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\Pdo;

use PDOException;
use RuntimeException;

class PdoMysqlErrorAnalyzer extends PdoErrorAnalyzer {
	
	protected static array $codes = [
		// See https://dev.mysql.com/doc/refman/8.0/en/server-error-reference.html
		1049 => self::CODE_UNKNOWN_DATABASE,
	];
	
	protected function parse(PDOException $exception): void {
		/** @noinspection RegExpRedundantEscape */
		if( !preg_match('#SQLSTATE\[([^\]]+)\] \[([^\]]+)\] (.+)#', $exception->getMessage(), $values) ) {
			throw new RuntimeException('Invalid MySQL PDOException message, unable to parse information', 0, $exception);
		}
		$this->state = $values[1];
		$this->code = $exception->getCode();
		$this->message = $values[3];
	}
	
}
