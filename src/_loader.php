<?php
use Orpheus\SQLAdapter\SQLAdapter;
/**
 * SQL Adapter Library
 * 
 * SQL Adapter library brings sql adapters for DBMS
 */

require_once '_pdo.php';

SQLAdapter::registerAdapter('mysql', 'Orpheus\SQLAdapter\SQLAdapterMySQL');
SQLAdapter::registerAdapter('mssql', 'Orpheus\SQLAdapter\SQLAdapterMSSQL');
SQLAdapter::registerAdapter('pgsql', 'Orpheus\SQLAdapter\SQLAdapterPGSQL');
