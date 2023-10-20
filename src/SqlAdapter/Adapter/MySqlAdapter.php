<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\SqlAdapter\Adapter;

use Exception;
use Orpheus\SqlAdapter\AbstractSqlAdapter;
use PDO;

/**
 * The MYSQL Adapter class
 *
 * This class is the sql adapter for MySQL.
 */
class MySqlAdapter extends AbstractSqlAdapter {
	
	/**
	 * Select defaults options
	 *
	 * @var array
	 */
	protected static array $selectDefaults = [
		'what'     => '',// table.* => All fields
		'join'     => '',// No join
		'where'  => '',// Additional Whereclause
		'orderby'  => '',// Ex: Field1 ASC, Field2 DESC
		'groupby'  => '',// Ex: Field
		'number'   => -1,// -1 => All
		'offset'   => 0,// 0 => The start
		'output' => AbstractSqlAdapter::RETURN_ARRAY_ASSOC,// Associative Array
		'alias'    => null,// No alias
		'distinct' => null,// No remove of duplicates
	];
	
	/**
	 * Update defaults options
	 *
	 * @var array
	 */
	protected static array $updateDefaults = [
		'lowpriority' => false,//false => Not low priority
		'ignore'      => false,//false => Not ignore errors
		'where'  => '',//Additional Whereclause
		'orderby'     => '',//Ex: Field1 ASC, Field2 DESC
		'number'      => -1,//-1 => All
		'offset'      => 0,//0 => The start
		'output' => AbstractSqlAdapter::NUMBER,//Number of updated lines
	];
	
	/**
	 * Delete defaults options
	 *
	 * @var array
	 */
	protected static array $deleteDefaults = [
		'lowpriority' => false,//false => Not low priority
		'quick'       => false,//false => Not merge index leaves
		'ignore'      => false,//false => Not ignore errors
		'where'  => '',//Additional Whereclause
		'orderby'     => '',//Ex: Field1 ASC, Field2 DESC
		'number'      => -1,//-1 => All
		'offset'      => 0,//0 => The start
		'output' => AbstractSqlAdapter::NUMBER,//Number of deleted lines
	];
	
	/**
	 * Insert defaults options
	 *
	 * @var array
	 */
	protected static array $insertDefaults = [
		'lowpriority' => false,//false => Not low priority
		'delayed'     => false,//false => Not delayed
		'ignore'      => false,//false => Not ignore errors
		'into'        => true,//true => INSERT INTO
		'output' => AbstractSqlAdapter::NUMBER,//Number of inserted lines
	];
	
	/**
	 * Select data from tables in database
	 *
	 * @param array $options The options used to build the query.
	 * @return mixed Mixed return, depending on the 'output' option.
	 * @throws Exception
	 * @see http://dev.mysql.com/doc/refman/5.0/en/select.html
	 */
	public function select(array $options = []): mixed {
		$options += self::$selectDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		$output = intval($options['output']);
		if( $output === static::RETURN_ARRAY_FIRST ) {
			$options['number'] = 1;
		}
		$isFromTable = $options['table'][0] !== '(';
		$TABLE = $isFromTable ? $this->escapeIdentifier($options['table']) : $options['table'];
		// Auto-satisfy join queries
		if( empty($options['what']) ) {
			$options['what'] = $isFromTable ? ($options['alias'] ?: $TABLE) . '.*' : '*';
		} elseif( is_array($options['what']) ) {
			$options['what'] = implode(', ', $options['what']);
		}
		$WHAT = $options['what'];
		$DISTINCT = $options['distinct'] ? 'DISTINCT' : '';
		$ALIAS = !empty($options['alias']) ? $options['alias'] : '';
		$where = $this->formatWhere($options['where']);
		$groupBy = !empty($options['groupby']) ? 'GROUP BY ' . $options['groupby'] : '';
		$orderBy = !empty($options['orderby']) ? 'ORDER BY ' . $options['orderby'] : '';
		$HAVING = !empty($options['having']) ? 'HAVING ' . (is_array($options['having']) ? implode(' AND ', $options['having']) : $options['where']) : '';
		$LIMIT = $options['number'] > 0 ? 'LIMIT ' .
			($options['offset'] > 0 ? $options['offset'] . ', ' : '') . $options['number'] : '';
		$JOIN = $this->parseJoin($options);
		
		$QUERY = "SELECT {$DISTINCT} {$WHAT} FROM {$TABLE} {$ALIAS} {$JOIN} {$where} {$groupBy} {$HAVING} {$orderBy} {$LIMIT}";
		if( $output === static::SQL_QUERY ) {
			return $QUERY;
		}
		$results = $this->query($QUERY, ($output === static::STATEMENT) ? AbstractSqlAdapter::QUERY_STATEMENT : AbstractSqlAdapter::QUERY_FETCH_ALL);
		if( $output === static::ARR_OBJECTS ) {
			foreach( $results as &$r ) {
				$r = (object) $r;//stdClass
			}
		}
		
		return ($results && $output === static::RETURN_ARRAY_FIRST) ? $results[0] : $results;
	}
	
	/**
	 * Update something in database
	 * Using pdo_query(), It parses the query from an array to a UPDATE query.
	 *
	 * @param array $options The options used to build the query.
	 * @return int The number of affected rows.
	 * @throws Exception
	 * @see http://dev.mysql.com/doc/refman/5.0/en/update.html
	 */
	public function update(array $options = []): int {
		$options += self::$updateDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		if( empty($options['what']) ) {
			throw new Exception('No field');
		}
		$OPTIONS = (!empty($options['lowpriority'])) ? ' LOW_PRIORITY' : '';
		$OPTIONS .= (!empty($options['ignore'])) ? ' IGNORE' : '';
		
		$WHAT = $this->formatFieldList($options['what']);
		$where = $this->formatWhere($options['where']);
		$ORDER_BY = !empty($options['orderby']) ? 'ORDER BY ' . $options['orderby'] : '';
		$LIMIT = ($options['number'] > 0) ? 'LIMIT ' .
			(($options['offset'] > 0) ? $options['offset'] . ', ' : '') . $options['number'] : '';
		$TABLE = static::escapeIdentifier($options['table']);
		
		$QUERY = "UPDATE {$OPTIONS} {$TABLE} SET {$WHAT} {$where} {$ORDER_BY} {$LIMIT}";
		if( $options['output'] === static::SQL_QUERY ) {
			return $QUERY;
		}
		
		return $this->query($QUERY, AbstractSqlAdapter::PROCESS_EXEC);
	}
	
	/**
	 * Insert something in database
	 *
	 * @param array $options The options used to build the query.
	 * @return int The number of inserted rows.
	 *
	 * It parses the query from an array to a INSERT query.
	 * Accept only the String syntax for what option.
	 * @throws Exception
	 */
	public function insert(array $options = []): mixed {
		$options += self::$insertDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		if( empty($options['what']) ) {
			throw new Exception('No field');
		}
		$queryOptions = (!empty($options['lowpriority'])) ? ' LOW_PRIORITY' : (!empty($options['delayed']) ? ' DELAYED' : '');
		$queryOptions .= (!empty($options['ignore'])) ? ' IGNORE' : '';
		$queryOptions .= (!empty($options['into'])) ? ' INTO' : '';
		
		$COLS = $WHAT = '';
		//Is an array
		if( is_array($options['what']) ) {
			//Is an indexed array of fields Arrays
			if( !empty($options['what'][0]) ) {
				// Quoted as escapeIdentifier()
				$COLS = '(`' . implode('`, `', array_keys($options['what'][0])) . '`)';
				foreach( $options['what'] as $row ) {
					$WHAT .= (!empty($WHAT) ? ', ' : '') . '(' . implode(', ', $row) . ')';
				}
				$WHAT = 'VALUES ' . $WHAT;
				//Is associative fields Arrays
			} else {
				$WHAT = 'SET ' . $this->formatFieldList($options['what']);
			}
			
			//Is a string
		} else {
			$WHAT = $options['what'];
		}
		$TABLE = static::escapeIdentifier($options['table']);
		
		$QUERY = "INSERT {$queryOptions} {$TABLE} {$COLS} {$WHAT}";
		if( $options['output'] === static::SQL_QUERY ) {
			return $QUERY;
		}
		
		return $this->query($QUERY, AbstractSqlAdapter::PROCESS_EXEC);
	}
	
	/**
	 * Delete something in database
	 *
	 * @param array $options The options used to build the query.
	 * @return int The number of deleted rows.
	 *
	 * It parses the query from an array to a DELETE query.
	 * @throws Exception
	 */
	public function delete(array $options = []): int {
		$options += self::$deleteDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		$OPTIONS = (!empty($options['lowpriority'])) ? ' LOW_PRIORITY' : '';
		$OPTIONS .= (!empty($options['quick'])) ? ' QUICK' : '';
		$OPTIONS .= (!empty($options['ignore'])) ? ' IGNORE' : '';
		$where = $this->formatWhere($options['where']);
		$orderBy = (!empty($options['orderby'])) ? 'ORDER BY ' . $options['orderby'] : '';
		$LIMIT = ($options['number'] > 0) ? 'LIMIT ' .
			(($options['offset'] > 0) ? $options['offset'] . ', ' : '') . $options['number'] : '';
		$TABLE = static::escapeIdentifier($options['table']);
		
		$QUERY = "DELETE {$OPTIONS} FROM {$TABLE} {$where} {$orderBy} {$LIMIT}";
		if( $options['output'] === static::SQL_QUERY ) {
			return $QUERY;
		}
		
		return $this->query($QUERY, AbstractSqlAdapter::PROCESS_EXEC);
	}
	
	protected function formatWhere(mixed $where): string {
		return $where ? 'WHERE ' . (is_array($where) ? implode(' AND ', $where) : $where) : '';
	}
	
	public function escapeIdentifier(string $identifier): string {
		return '`' . str_replace('.', '`.`', $identifier) . '`';
	}
	
	/**
	 * Parse join option to generate join
	 */
	protected function parseJoin($options): string {
		$joinList = $options['join'];
		if( is_object($joinList) ) {
			$joinList = [$joinList];
		}
		if( !is_array($joinList) ) {
			// Join as string
			return $joinList;
		}
		$joinStr = '';
		// Array of string or array of object
		foreach( $options['join'] as $join ) {
			if( is_object($join) ) {
				/**
				 * Fields: table, alias, condition, mandatory
				 * All are mandatories
				 */
				$join = sprintf(
					'%s JOIN %s %s ON %s',
					$join->mandatory ? 'INNER' : 'LEFT',
					$this->escapeIdentifier($join->table),
					$join->alias,
					$join->condition
				);
			}
			$joinStr .= ($joinStr ? ', ' : '') . $join;
		}
		
		return $joinStr;
	}
	
	/**
	 * Get the last inserted ID
	 * It requires a successful call of insert() !
	 *
	 * @param string $table The table to get the last inserted id.
	 * @return string|null The last inserted id value.
	 */
	public function lastId(string $table): ?string {
		return $this->query('SELECT LAST_INSERT_ID();', AbstractSqlAdapter::FETCH_FIRST_COLUMN);
	}
	
	protected function connect(): void {
		$config = $this->config;
		$this->pdo = new PDO(
			"mysql:dbname={$config['dbname']};host={$config['host']}" . (!empty($config['port']) ? ';port=' . $config['port'] : ''),
			$config['user'], $config['passwd'],
			[PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8', PDO::MYSQL_ATTR_DIRECT_QUERY => true]
		);
		$this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
	}
	
	/**
	 * Get the driven string
	 */
	public static function getDriver(): string {
		return 'mysql';
	}
	
}
