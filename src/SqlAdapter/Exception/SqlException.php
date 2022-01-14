<?php
/**
 * SqlException
 */

namespace Orpheus\SqlAdapter\Exception;

use Exception;
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
	 *
	 * @param string $message
	 * @param string $action
	 * @param PDOException $original
	 */
	public function __construct($message = null, $action = null, $original = null) {
		parent::__construct($message, 0, $original);
		$this->action = $action;
	}
	
	/**
	 * Get the action
	 *
	 * @return ?string
	 */
	public function getAction(): ?string {
		return $this->action;
	}
	
	/**
	 * Get the exception as report
	 *
	 * @return string
	 */
	public function getReport() {
		return $this->getText();
	}
	
	/**
	 * Get the exception as report
	 *
	 * @return string
	 */
	public function getText() {
		return $this->getMessage();
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @see Exception::__toString()
	 */
	public function __toString() {
		try {
			return $this->getText();
		} catch( Exception $e ) {
			if( ERROR_LEVEL === DEV_LEVEL ) {
				die('A fatal error occurred in UserException::__toString() :<br />' . $e->getMessage());
			}
			die('A fatal error occurred, please report it to an admin.<br />Une erreur fatale est survenue, veuillez contacter un administrateur.<br />');
		}
		
		return '';
	}
	
}