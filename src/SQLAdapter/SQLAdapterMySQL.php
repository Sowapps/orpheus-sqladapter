<?php
/**
 * SQLAdapterMySQL
 */

namespace Orpheus\SQLAdapter;

use Exception;
use PDO;

/**
 * The MYSQL Adapter class
 *
 * This class is the sql adapter for MySQL.
 */
class SQLAdapterMySQL extends SQLAdapter {
	
	/**
	 * Select defaults options
	 *
	 * @var array
	 */
	protected static $selectDefaults = [
		'what'     => '',// table.* => All fields
		'join'     => '',// No join
		'where'    => '',// Additionnal Whereclause
		'orderby'  => '',// Ex: Field1 ASC, Field2 DESC
		'groupby'  => '',// Ex: Field
		'number'   => -1,// -1 => All
		'offset'   => 0,// 0 => The start
		'output'   => SQLAdapter::ARR_ASSOC,// Associative Array
		'alias'    => null,// No alias
		'distinct' => null,// No remove of duplicates
	];
	
	/**
	 * Update defaults options
	 *
	 * @var array
	 */
	protected static $updateDefaults = [
		'lowpriority' => false,//false => Not low priority
		'ignore'      => false,//false => Not ignore errors
		'where'       => '',//Additionnal Whereclause
		'orderby'     => '',//Ex: Field1 ASC, Field2 DESC
		'number'      => -1,//-1 => All
		'offset'      => 0,//0 => The start
		'output'      => SQLAdapter::NUMBER,//Number of updated lines
	];
	
	/**
	 * Delete defaults options
	 *
	 * @var array
	 */
	protected static $deleteDefaults = [
		'lowpriority' => false,//false => Not low priority
		'quick'       => false,//false => Not merge index leaves
		'ignore'      => false,//false => Not ignore errors
		'where'       => '',//Additionnal Whereclause
		'orderby'     => '',//Ex: Field1 ASC, Field2 DESC
		'number'      => -1,//-1 => All
		'offset'      => 0,//0 => The start
		'output'      => SQLAdapter::NUMBER,//Number of deleted lines
	];
	
	/**
	 * Insert defaults options
	 *
	 * @var array
	 */
	protected static $insertDefaults = [
		'lowpriority' => false,//false => Not low priority
		'delayed'     => false,//false => Not delayed
		'ignore'      => false,//false => Not ignore errors
		'into'        => true,//true => INSERT INTO
		'output'      => SQLAdapter::NUMBER,//Number of inserted lines
	];
	
	/**
	 * Select something from database
	 *
	 * @param array $options The options used to build the query.
	 * @return mixed Mixed return, depending on the 'output' option.
	 * @throws Exception
	 * @see http://dev.mysql.com/doc/refman/5.0/en/select.html
	 *
	 * Using pdo_query(), It parses the query from an array to a SELECT query.
	 */
	public function select(array $options = []) {
		$options += self::$selectDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		$output = intval($options['output']);
		if( !$options['number'] && $output === static::ARR_FIRST ) {
			$options['number'] = 1;
		}
		$isFromTable = $options['table'][0] != '(';
		$TABLE = $isFromTable ? static::escapeIdentifier($options['table']) : $options['table'];
		// Auto-satisfy join queries
		if( empty($options['what']) ) {
			$options['what'] = $isFromTable ? (!empty($options['alias']) ? $options['alias'] : $TABLE) . '.*' : '*';
		} elseif( is_array($options['what']) ) {
			$options['what'] = implode(', ', $options['what']);
		}
		$WHAT = $options['what'];
		$DISTINCT = $options['distinct'] ? 'DISTINCT' : '';
		$ALIAS = !empty($options['alias']) ? $options['alias'] : '';
		$WC = $options['where'] ? 'WHERE ' . (is_array($options['where']) ? implode(' AND ', $options['where']) : $options['where']) : '';
		$GROUPBY = !empty($options['groupby']) ? 'GROUP BY ' . $options['groupby'] : '';
		$ORDERBY = !empty($options['orderby']) ? 'ORDER BY ' . $options['orderby'] : '';
		$HAVING = !empty($options['having']) ? 'HAVING ' . (is_array($options['having']) ? implode(' AND ', $options['having']) : $options['where']) : '';
		$LIMIT = $options['number'] > 0 ? 'LIMIT ' .
			($options['offset'] > 0 ? $options['offset'] . ', ' : '') . $options['number'] : '';
		$JOIN = $this->parseJoin($options);
		
		$QUERY = "SELECT {$DISTINCT} {$WHAT} FROM {$TABLE} {$ALIAS} {$JOIN} {$WC} {$GROUPBY} {$HAVING} {$ORDERBY} {$LIMIT}";
		if( $output === static::SQLQUERY ) {
			return $QUERY;
		}
		$results = $this->query($QUERY, ($output === static::STATEMENT) ? PDOSTMT : PDOFETCHALL);
		if( $output === static::ARR_OBJECTS ) {
			foreach( $results as &$r ) {
				$r = (object) $r;//stdClass
			}
		}
		return (!empty($results) && $output === static::ARR_FIRST) ? $results[0] : $results;
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @param string $identifier The identifier to escape
	 * @see \Orpheus\SQLAdapter\SQLAdapter::escapeIdentifier()
	 *
	 */
	public function escapeIdentifier($identifier) {
		return '`' . str_replace('.', '`.`', $identifier) . '`';
	}
	
	/**
	 * Parse join option to generate join
	 *
	 * @param $options
	 * @return array|string
	 */
	protected function parseJoin($options) {
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
	 * Update something in database
	 *
	 * @param array $options The options used to build the query.
	 * @return int The number of affected rows.
	 * @throws Exception
	 * @see http://dev.mysql.com/doc/refman/5.0/en/update.html
	 *
	 * Using pdo_query(), It parses the query from an array to a UPDATE query.
	 */
	public function update(array $options = []) {
		$options += self::$updateDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		if( empty($options['what']) ) {
			throw new Exception('No field');
		}
		$OPTIONS = '';
		$OPTIONS .= (!empty($options['lowpriority'])) ? ' LOW_PRIORITY' : '';
		$OPTIONS .= (!empty($options['ignore'])) ? ' IGNORE' : '';
		
		$WHAT = $this->formatFieldList($options['what']);
		$WC = !empty($options['where']) ? 'WHERE ' . $options['where'] : '';
		$ORDER_BY = !empty($options['orderby']) ? 'ORDER BY ' . $options['orderby'] : '';
		$LIMIT = ($options['number'] > 0) ? 'LIMIT ' .
			(($options['offset'] > 0) ? $options['offset'] . ', ' : '') . $options['number'] : '';
		$TABLE = static::escapeIdentifier($options['table']);
		
		$QUERY = "UPDATE {$OPTIONS} {$TABLE} SET {$WHAT} {$WC} {$ORDER_BY} {$LIMIT}";
		if( $options['output'] == static::SQLQUERY ) {
			return $QUERY;
		}
		return $this->query($QUERY, PDOEXEC);
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
	public function insert(array $options = []) {
		$options += self::$insertDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		if( empty($options['what']) ) {
			throw new Exception('No field');
		}
		$OPTIONS = '';
		$OPTIONS .= (!empty($options['lowpriority'])) ? ' LOW_PRIORITY' : (!empty($options['delayed']) ? ' DELAYED' : '');
		$OPTIONS .= (!empty($options['ignore'])) ? ' IGNORE' : '';
		$OPTIONS .= (!empty($options['into'])) ? ' INTO' : '';
		
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
		
		$QUERY = "INSERT {$OPTIONS} {$TABLE} {$COLS} {$WHAT}";
		if( $options['output'] == static::SQLQUERY ) {
			return $QUERY;
		}
		return $this->query($QUERY, PDOEXEC);
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
	public function delete(array $options = []) {
		$options += self::$deleteDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		$OPTIONS = '';
		$OPTIONS .= (!empty($options['lowpriority'])) ? ' LOW_PRIORITY' : '';
		$OPTIONS .= (!empty($options['quick'])) ? ' QUICK' : '';
		$OPTIONS .= (!empty($options['ignore'])) ? ' IGNORE' : '';
		$WC = (!empty($options['where'])) ? 'WHERE ' . $options['where'] : '';
		$ORDER_BY = (!empty($options['orderby'])) ? 'ORDER BY ' . $options['orderby'] : '';
		$LIMIT = ($options['number'] > 0) ? 'LIMIT ' .
			(($options['offset'] > 0) ? $options['offset'] . ', ' : '') . $options['number'] : '';
		$TABLE = static::escapeIdentifier($options['table']);
		
		$QUERY = "DELETE {$OPTIONS} FROM {$TABLE} {$WC} {$ORDER_BY} {$LIMIT}";
		if( $options['output'] == static::SQLQUERY ) {
			return $QUERY;
		}
		return $this->query($QUERY, PDOEXEC);
	}
	
	/**
	 * Get the last inserted ID
	 *
	 * @param string $table The table to get the last inserted id.
	 * @return mixed The last inserted id value.
	 *
	 * It requires a successful call of insert() !
	 */
	public function lastID($table) {
		return $this->query('SELECT LAST_INSERT_ID();', PDOFETCHFIRSTCOL);
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @param array $config
	 * @see \Orpheus\SQLAdapter\SQLAdapter::connect()
	 *
	 */
	protected function connect(array $config) {
		$this->pdo = new PDO(
			"mysql:dbname={$config['dbname']};host={$config['host']}" . (!empty($config['port']) ? ';port=' . $config['port'] : ''),
			$config['user'], $config['passwd'],
			[PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8', PDO::MYSQL_ATTR_DIRECT_QUERY => true]
		);
		$this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
	}
	
	/**
	 * Get the driven string
	 *
	 * @return string
	 */
	public static function getDriver() {
		return 'mysql';
	}
}
