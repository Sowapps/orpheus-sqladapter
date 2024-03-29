<?php
/**
 * @author Florent HAZARD <f.hazard@sowapps.com>
 */

namespace Orpheus\SqlAdapter\Adapter;

use Exception;
use Orpheus\SqlAdapter\AbstractSqlAdapter;
use PDO;

/**
 * The MS Sql Adapter class
 *
 * This class is the sql adapter for MSSQL.
 *
 * Install method for debian:
 * http://atutility.com/2007/09/14/install-pdo-pdo_sqlite-pdo_dblib-pdo_mysql
 */
class MsSqlAdapter extends AbstractSqlAdapter {
	
	/**
	 * Select defaults options
	 *
	 * @var array
	 */
	protected static array $selectDefaults = [
		'what'           => '',//table.* => All fields
		'join'           => '',// No join
		'where'  => '',//Additional Whereclause
		'orderby'        => '',//Ex: Field1 ASC, Field2 DESC
		'groupby'        => '',//Ex: Field
		'number'         => -1,//-1 => All
		'number_percent' => false,// false => No Percent option
		'offset'         => 0,//0 => The start
		'output' => AbstractSqlAdapter::RETURN_ARRAY_ASSOC,//Associative Array
	];
	
	/**
	 * Update defaults options
	 *
	 * @var array
	 */
	protected static array $updateDefaults = [
		'lowpriority'    => false,//false => Not low priority
		'ignore'         => false,//false => Not ignore errors
		'where'  => '',//Additional Whereclause
		'orderby'        => '',//Ex: Field1 ASC, Field2 DESC
		'number'         => -1,//-1 => All
		'number_percent' => false,// false => No Percent option
		'offset'         => 0,//0 => The start
		'output' => AbstractSqlAdapter::NUMBER,//Number of updated lines
	];
	
	/**
	 * Delete defaults options
	 *
	 * @var array
	 */
	protected static array $deleteDefaults = [
		'lowpriority'    => false,//false => Not low priority
		'quick'          => false,//false => Not merge index leaves
		'ignore'         => false,//false => Not ignore errors
		'where'  => '',//Additional Whereclause
		'orderby'        => '',//Ex: Field1 ASC, Field2 DESC
		'number'         => -1,//-1 => All
		'number_percent' => false,// false => No Percent option
		'offset'         => 0,//0 => The start
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
	 *
	 * {@inheritDoc}
	 * @param array $options The options used to build the query
	 * @return mixed|string
	 * @throws Exception
	 * @see http://msdn.microsoft.com/en-us/library/aa259187%28v=sql.80%29.aspx
	 * @see AbstractSqlAdapter::select()
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
		$TABLE = $isFromTable ? static::escapeIdentifier($options['table']) : $options['table'];
		// Auto-satisfy join queries
		if( empty($options['what']) ) {
			$options['what'] = '*';
		}
		$idField = !empty($options['idField']) ? $options['idField'] : $this->defaultIdField;
		$OPTIONS = '';
		$WHAT = is_array($options['what']) ? implode(', ', $options['what']) : $options['what'];
		$WC = $options['where'] ? 'WHERE ' . (is_array($options['where']) ? implode(' AND ', $options['where']) : $options['where']) : '';
		$ORDERBY = !empty($options['orderby']) ? 'ORDER BY ' . $options['orderby'] : '';
		
		if( $options['number'] > 0 ) {
			// ORDER BY is required
			$LIMIT_WC = ($options['offset'] > 0) ? $options['offset'] . ' AND ' . ($options['offset'] + $options['number']) : '<= ' . $options['number'];
			if( !$ORDERBY ) {
				// Over is mandatory with row_number()
				$ORDERBY = 'ORDER BY ' . $idField;
			}
			$QUERY = "SELECT * FROM ( SELECT {$WHAT}, row_number() OVER ({$ORDERBY}) AS rownum FROM {$TABLE} {$WC} ) AS a WHERE a.rownum {$LIMIT_WC};";
			
		} else {
			$QUERY = "SELECT {$OPTIONS} {$WHAT} FROM {$options['table']} {$WC} {$ORDERBY};";
		}
		if( $output === static::SQL_QUERY ) {
			return $QUERY;
		}
		$results = $this->query($QUERY, $output === static::STATEMENT ? AbstractSqlAdapter::QUERY_STATEMENT : AbstractSqlAdapter::QUERY_FETCH_ALL);
		if( $output === static::ARR_OBJECTS ) {
			foreach( $results as &$r ) {
				$r = (object) $r;//stdClass
			}
		}
		
		return ($results && $output === static::RETURN_ARRAY_FIRST) ? $results[0] : $results;
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @see http://msdn.microsoft.com/en-us/library/ms177523.aspx
	 * @param array $options The options used to build the query
	 * @throws Exception
	 */
	public function update(array $options = []): int {
		$options += self::$updateDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		if( empty($options['what']) ) {
			throw new Exception('No field');
		}
		$idField = !empty($options['idField']) ? $options['idField'] : $this->defaultIdField;
		$WC = (!empty($options['where'])) ? 'WHERE ' . $options['where'] : '';
		$ORDERBY = !empty($options['orderby']) ? 'ORDER BY ' . $options['orderby'] : '';
		
		$WHAT = $this->formatFieldList($options['what']);
		
		if( $options['number'] > 0 ) {
			// ORDER BY is required
			$LIMIT_WC = ($options['offset'] > 0) ? $options['offset'] . ' AND ' . ($options['offset'] + $options['number']) : '<= ' . $options['number'];
			if( !$ORDERBY ) {
				// Over is mandatory with row_number()
				$ORDERBY = 'ORDER BY ' . $idField;
			}
			$QUERY = "WITH a AS ( SELECT *, row_number() OVER ({$ORDERBY}) AS rownum FROM {$options['table']} {$WC} )
				UPDATE a SET {$WHAT} WHERE a.rownum {$LIMIT_WC};";
		} else {
			$QUERY = "UPDATE {$options['table']} SET {$WHAT} {$WC} {$ORDERBY};";
		}
		
		if( $options['output'] === static::SQL_QUERY ) {
			return $QUERY;
		}
		
		return $this->query($QUERY, AbstractSqlAdapter::PROCESS_EXEC);
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @param array $options The options used to build the query
	 * @throws Exception
	 * @see AbstractSqlAdapter::insert()
	 * @see http://msdn.microsoft.com/en-us/library/ms174335.aspx
	 */
	public function insert(array $options = []): int {
		$options += self::$insertDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		if( empty($options['what']) ) {
			throw new Exception('No field');
		}
		$OPTIONS = (!empty($options['into'])) ? ' INTO' : '';
		
		$COLS = $WHAT = '';
		// Is an array
		if( is_array($options['what']) ) {
			// Is an associative array
			if( !isset($options['what'][0]) ) {
				$options['what'] = [0 => $options['what']];
			}
			// Indexed array to values string
			// Quoted as escapeIdentifier()
			$COLS = '("' . implode('", "', array_keys($options['what'][0])) . '")';
			foreach( $options['what'] as $row ) {
				$WHAT .= (!empty($WHAT) ? ', ' : '') . '(' . implode(', ', $row) . ')';
			}
			$WHAT = 'VALUES ' . $WHAT;
			
			//Is a string
		} else {
			$WHAT = $options['what'];
		}
		
		$QUERY = "INSERT {$OPTIONS} {$options['table']} {$COLS} {$WHAT};";
		// SELECT SCOPE_IDENTITY() LAST_ID;
		if( $options['output'] === static::SQL_QUERY ) {
			return $QUERY;
		}
		
		return $this->query($QUERY, AbstractSqlAdapter::PROCESS_EXEC);
	}
	
	/**
	 * @param array $options The options used to build the query
	 * @throws Exception
	 * @see http://msdn.microsoft.com/en-us/library/ms189835.aspx
	 */
	public function delete(array $options = []): int {
		$options += self::$deleteDefaults;
		if( empty($options['table']) ) {
			throw new Exception('Empty table option');
		}
		$idField = !empty($options['idField']) ? $options['idField'] : $this->defaultIdField;
		$WC = (!empty($options['where'])) ? 'WHERE ' . $options['where'] : '';
		if( empty($options['orderby']) ) {
			$options['orderby'] = $this->defaultIdField;
		}
		$ORDERBY = !empty($options['orderby']) ? 'ORDER BY ' . $options['orderby'] : '';
		
		if( $options['number'] > 0 ) {
			// ORDER BY is required
			$LIMIT_WC = ($options['offset'] > 0) ? $options['offset'] . ' AND ' . ($options['offset'] + $options['number']) : '<= ' . $options['number'];
			if( !$ORDERBY ) {
				// Over is mandatory with row_number()
				$ORDERBY = 'ORDER BY ' . $idField;
			}
			$QUERY = "WITH a AS ( SELECT *, row_number() OVER ({$ORDERBY}) AS rownum FROM {$options['table']} {$WC} )
				DELETE FROM a WHERE a.rownum {$LIMIT_WC};";
			
		} else {
			$QUERY = "DELETE FROM {$options['table']} {$WC} {$ORDERBY};";
		}
		
		if( $options['output'] === static::SQL_QUERY ) {
			return $QUERY;
		}
		
		return $this->query($QUERY, AbstractSqlAdapter::PROCESS_EXEC);
	}
	
	/**
	 * Get the last inserted ID
	 * It requires a successful call of insert() !
	 *
	 * @param string $table The table to get the last inserted id
	 * @return string|null The last inserted id value
	 */
	public function lastId(string $table): ?string {
		$r = $this->query("SELECT SCOPE_IDENTITY() AS LAST_ID;", AbstractSqlAdapter::QUERY_FETCH_ONE);
		
		return $r['LAST_ID'] ?? null;
	}
	
	protected function connect(): void {
		$config = $this->config;
		$this->pdo = new PDO(
			"dblib:dbname={$config['dbname']};host={$config['host']}" . (!empty($config['port']) ? ':' . $config['port'] : ''),
			$config['user'], $config['passwd']
		);
		$this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
	}
	
	/**
	 * Get the driven string
	 */
	public static function getDriver(): string {
		return 'dblib';
	}
	
}
