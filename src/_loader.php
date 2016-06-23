<?php
use Orpheus\SQLAdapter\SQLAdapter;

/**
 * SQL Adapter Library
 * 
 * SQL Adapter library brings sql adapters for DBMS
 */

require_once '_pdo.php';

SQLAdapter::registerAdapter('mysql', 'SQLAdapter_MySQL');
