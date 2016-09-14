<?php declare(strict_types = 1);

namespace Amp\Postgres\Test;

use Amp\Postgres\{ Connection, PgSqlConnection };

class PgSqlConnectionTest extends AbstractConnectionTest {
    /** @var resource PostgreSQL connection resource. */
    protected $handle;

    public function createConnection(string $connectionString): Connection {
        $this->handle = \pg_connect($connectionString);
        $socket = \pg_socket($this->handle);
    
        $result = \pg_query($this->handle, "CREATE TABLE test (domain VARCHAR(63), tld VARCHAR(63), PRIMARY KEY (domain, tld))");
    
        if (!$result) {
            $this->fail('Could not create test table.');
        }
    
        foreach ($this->getData() as $row) {
            $result = \pg_query_params($this->handle, "INSERT INTO test VALUES (\$1, \$2)", $row);
        
            if (!$result) {
                $this->fail('Could not insert test data.');
            }
        }
    
        return new PgSqlConnection($this->handle, $socket);
    }
    
    public function getConnectCallable(): callable {
        return [PgSqlConnection::class, 'connect'];
    }
    
    public function tearDown() {
        \pg_query($this->handle, "ROLLBACK");
        \pg_query($this->handle, "DROP TABLE test");
    }
}
