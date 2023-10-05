<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\Pdo;

use Exception;
use Orpheus\SqlAdapter\Exception\SqlException;
use PDOException;

abstract class PdoErrorAnalyzer {
	
	const CODE_UNKNOWN_DATABASE = 'unknownDatabase';
	
	protected static array $codes = [];
	
	protected PDOException $exception;
	
	protected string $state;
	
	protected int $code;
	
	protected string $message;
	
	/**
	 * PdoErrorAnalyzer constructor
	 */
	protected function __construct(PDOException $exception) {
		$this->exception = $exception;
		$this->parse($exception);
	}
	
	protected abstract function parse(PDOException $exception);
	
	public function getCodeReference(): ?string {
		return static::$codes[$this->code] ?? null;
	}
	
	public function getException(): PDOException {
		return $this->exception;
	}
	
	public function getCode(): int {
		return $this->code;
	}
	
	public function getMessage(): string {
		return $this->message;
	}
	
	public static function from(Exception $exception): static {
		return new static($exception instanceof PDOException ? $exception : $exception->getPrevious());
	}
	
	public static function fromDriver(Exception $exception, ?string $driver = null): static {
		$exception = $exception instanceof PDOException ? $exception : $exception->getPrevious();
		
		return match ($driver) {
			'mysql' => new static($exception),
			default => throw new SqlException(sprintf('Unknown driver %s', $driver), 'Getting driver'),
		};
	}
	
}
