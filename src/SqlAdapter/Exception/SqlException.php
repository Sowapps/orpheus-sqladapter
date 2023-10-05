<?php
/**
 * SqlException
 */

namespace Orpheus\SqlAdapter\Exception;

use PDOException;
use RuntimeException;

/**
 * The SQL exception class
 *
 * This exception is thrown when an occurred caused by the SQL DBMS (or DBMS tools).
 */
class SqlException extends RuntimeException {
	
	/**
	 * Action in progress while getting this exception
	 *
	 * @var string|null
	 */
	protected ?string $action;
	
	/**
	 * Constructor
	 */
	public function __construct(?string $message = null, ?string $action = null, ?PDOException $original = null) {
		parent::__construct($message, 0, $original);
		$this->action = $action;
	}
	
	/**
	 * Get the action
	 */
	public function getAction(): ?string {
		return $this->action;
	}
	
	/**
	 * Get the exception as report
	 */
	public function getReport(): string {
		return $this->getText();
	}
	
	/**
	 * Get the exception as report
	 */
	public function getText(): string {
		return $this->getMessage();
	}
	
	public function __toString(): string {
		return $this->getText();
	}
	
}
