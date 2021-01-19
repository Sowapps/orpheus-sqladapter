<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\Pdo;

use Exception;
use PDOException;

abstract class PdoErrorAnalyzer {
	
	const CODE_UNKNOWN_DATABASE = 'unknownDatabase';
	
	protected static $codes = [];
	
	/** @var PDOException */
	protected $exception;
	
	/** @var string */
	protected $state;
	
	/** @var int */
	protected $code;
	
	/** @var string */
	protected $message;
	
	/**
	 * PdoErrorAnalyzer constructor
	 *
	 * @param PDOException $exception
	 */
	protected function __construct(PDOException $exception) {
		$this->exception = $exception;
		$this->parse($exception);
	}
	
	protected abstract function parse(PDOException $exception);
	
	/**
	 * @return string
	 */
	public function getCodeReference() {
		return isset(static::$codes[$this->code]) ? static::$codes[$this->code] : null;
	}
	
	/**
	 * @return PDOException
	 */
	public function getException() {
		return $this->exception;
	}
	
	/**
	 * @return int
	 */
	public function getCode() {
		return $this->code;
	}
	
	/**
	 * @return string
	 */
	public function getMessage() {
		return $this->message;
	}
	
	public static function from(Exception $exception) {
		return new static($exception instanceof PDOException ? $exception : $exception->getPrevious());
	}
	
	/**
	 * @param Exception $exception
	 * @param null $driver
	 * @return PdoMysqlErrorAnalyzer|static
	 */
	public static function fromDriver(Exception $exception, $driver = null) {
		$exception = $exception instanceof PDOException ? $exception : $exception->getPrevious();
		switch( $driver ) {
			case 'mysql':
				return new PdoMysqlErrorAnalyzer($exception);
				break;
			default:
				throw new Exception(sprintf('Unknown driver %s', $driver));
				break;
		}
		
		return new static();
	}
	
}
