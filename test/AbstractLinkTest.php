<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Future;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Internal\PostgresHandleConnection;
use Amp\Postgres\PostgresByteA;
use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresLink;
use Amp\Postgres\PostgresQueryError;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\SqlQueryError;
use Amp\Sql\SqlResult;
use Amp\Sql\SqlStatement;
use Amp\Sql\SqlTransactionError;
use function Amp\async;

abstract class AbstractLinkTest extends AsyncTestCase
{
    protected const CREATE_QUERY = "CREATE TABLE test (domain VARCHAR(63) NOT NULL,
                                                       tld VARCHAR(63) NOT NULL,
                                                       keys INTEGER[] NOT NULL,
                                                       enabled BOOLEAN NOT NULL, 
                                                       number DOUBLE PRECISION NOT NULL,
                                                       nullable CHAR(1) DEFAULT NULL,
                                                       bytea BYTEA DEFAULT NULL,
                                                       json JSON DEFAULT NULL,
                                                       PRIMARY KEY (domain, tld))";
    protected const DROP_QUERY = "DROP TABLE IF EXISTS test";
    protected const INSERT_QUERY = 'INSERT INTO test VALUES ($1, $2, $3, $4, $5, $6, $7, $8)';
    protected const FIELD_COUNT = 8;

    protected PostgresExecutor $executor;

    private ?array $data = null;

    protected function getParams(): array
    {
        return $this->data ??= [
            ['amphp', 'org', [1], true, 3.14159, null, new PostgresByteA(\random_bytes(10)), \json_encode('string')],
            ['github', 'com', [1, 2, 3, 4, 5], false, 2.71828, null, new PostgresByteA(\str_repeat("\0", 10)), \json_encode([1, 2, 3])],
            ['google', 'com', [1, 2, 3, 4], true, 1.61803, null, new PostgresByteA(\random_bytes(42)), \json_encode(null)],
            ['php', 'net', [1, 2], false, 0.0, null, null, \json_encode((object) ['value' => 1])],
        ];
    }

    /**
     * @return array Start test data for database.
     */
    protected function getData(): array
    {
        return \array_map(fn (array $params) => \array_map(
            fn (mixed $param) => $param instanceof PostgresByteA ? $param->getData() : $param,
            $params,
        ), $this->getParams());
    }

    protected function verifyResult(SqlResult $result, array $data): void
    {
        $this->assertSame(self::FIELD_COUNT, $result->getColumnCount());

        $i = 0;
        foreach ($result as $row) {
            $this->assertSame($data[$i][0], $row['domain']);
            $this->assertSame($data[$i][1], $row['tld']);
            $this->assertSame($data[$i][2], $row['keys']);
            $this->assertSame($data[$i][3], $row['enabled']);
            $this->assertEqualsWithDelta($data[$i][4], $row['number'], 0.001);
            $this->assertNull($row['nullable']);
            $this->assertEquals(\json_decode($data[$i][7]), \json_decode($row['json']));
            ++$i;
        }
    }

    /**
     * @return PostgresLink Executor object to be tested.
     */
    abstract public function createLink(string $connectionString): PostgresLink;

    /**
     * Helper method to invoke the protected constructor of classes extending {@see PostgresHandleConnection}.
     *
     * @template T extends Connection
     *
     * @param class-string<T> $className
     * @param mixed ...$args Constructor arguments.
     *
     * @return T
     */
    protected function newConnection(string $className, mixed ...$args): PostgresHandleConnection
    {
        $reflection = new \ReflectionClass($className);
        $connection = $reflection->newInstanceWithoutConstructor();
        $reflection->getConstructor()->invokeArgs($connection, $args);
        return $connection;
    }

    public function setUp(): void
    {
        parent::setUp();
        $this->executor = $this->createLink('host=localhost user=postgres password=postgres');
    }

    public function tearDown(): void
    {
        parent::tearDown();
        $this->executor->close();
    }

    public function testQueryFetchRow(): void
    {
        $result = $this->executor->query("SELECT * FROM test");

        $data = $this->getData();
        while ($row = $result->fetchRow()) {
            self::assertSame(\array_shift($data), \array_values($row));
        }

        self::assertEmpty($data);
    }

    public function testQueryWithTupleResult()
    {
        $result = $this->executor->query("SELECT * FROM test");

        $data = $this->getData();

        $this->verifyResult($result, $data);

        $this->assertNull($result->getNextResult());
    }

    public function testMultipleQueryWithTupleResult()
    {
        $result = $this->executor->query("SELECT * FROM test; SELECT * FROM test");

        $data = $this->getData();

        $this->verifyResult($result, $data);

        $result = $result->getNextResult();

        $this->assertInstanceOf(SqlResult::class, $result);

        $this->verifyResult($result, $data);

        $this->assertNull($result->getNextResult());
    }

    public function testMultipleQueryWithCommandResultFirst()
    {
        $result = $this->executor->query("INSERT INTO test VALUES ('canon', 'jp', '{1}', true, 4.2, null, null, '3.1415926'); SELECT * FROM test");

        $this->assertSame(1, $result->getRowCount());

        $result = $result->getNextResult();

        $this->assertInstanceOf(SqlResult::class, $result);

        $data = $this->getData();
        $data[] = ['canon', 'jp', [1], true, 4.2, null, null, \json_encode(3.1415926)]; // Add inserted row to expected data.

        $this->verifyResult($result, $data);

        $this->assertNull($result->getNextResult());
    }

    public function testMultipleQueryWithCommandResultSecond()
    {
        $result = $this->executor->query("SELECT * FROM test; INSERT INTO test VALUES ('canon', 'jp', '{1}', true, 4.2)");

        $data = $this->getData();

        $this->verifyResult($result, $data);

        $result = $result->getNextResult();

        $this->assertSame(1, $result->getRowCount());

        $this->assertNull($result->getNextResult());
    }

    public function testQueryWithUnconsumedTupleResult()
    {
        $result = $this->executor->query("SELECT * FROM test");

        $this->assertInstanceOf(SqlResult::class, $result);

        unset($result); // Force destruction of result object.

        $result = $this->executor->query("SELECT * FROM test");

        $this->assertInstanceOf(SqlResult::class, $result);

        $data = $this->getData();

        $this->verifyResult($result, $data);
    }

    public function testQueryWithCommandResult(): void
    {
        $result = $this->executor->query("INSERT INTO test VALUES ('canon', 'jp', '{1}', true, 4.2)");

        $this->assertSame(1, $result->getRowCount());
    }

    public function testQueryWithEmptyQuery(): void
    {
        $this->expectException(SqlQueryError::class);
        $this->executor->query('');
    }

    public function testQueryWithSyntaxError()
    {
        /** @var SqlResult $result */
        try {
            $result = $this->executor->query("SELECT & FROM test");
            $this->fail(\sprintf("An instance of %s was expected to be thrown", PostgresQueryError::class));
        } catch (PostgresQueryError $exception) {
            $diagnostics = $exception->getDiagnostics();
            $this->assertArrayHasKey("sqlstate", $diagnostics);
        }
    }

    public function testPrepare()
    {
        $query = "SELECT * FROM test WHERE domain=\$1";

        $statement = $this->executor->prepare($query);

        $this->assertSame($query, $statement->getQuery());

        $data = $this->getData()[0];

        $result = $statement->execute([$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    public function testPrepareWithCommandResult()
    {
        $query = "INSERT INTO test (domain, tld, keys, enabled, number, json) VALUES (:domain, :tld, :keys, :enabled, :number, :json)";

        $statement = $this->executor->prepare($query);

        $fields = [
            'domain' => 'canon',
            'tld' => 'jp',
            'keys' => [1],
            'enabled' => true,
            'number' => 1,
            'nullable' => null,
            'bytea' => null,
            'json' => '[1,2,3]',
        ];

        $result = $statement->execute($fields);

        $this->assertSame(1, $result->getRowCount());

        $result = $this->executor->execute('SELECT * FROM test WHERE domain=? AND tld=?', ['canon', 'jp']);

        $this->verifyResult($result, [\array_values($fields)]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParams()
    {
        $query = "SELECT * FROM test WHERE domain=:domain AND tld=:tld";

        $statement = $this->executor->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        $result = $statement->execute(['domain' => $data[0], 'tld' => $data[1]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithUnnamedParams()
    {
        $query = "SELECT * FROM test WHERE domain=? AND tld=?";

        $statement = $this->executor->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        $result = $statement->execute([$data[0], $data[1]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareWithNamedParamsWithDataAppearingAsNamedParam()
    {
        $query = "SELECT * FROM test WHERE domain=:domain OR domain=':domain'";

        $statement = $this->executor->prepare($query);

        $data = $this->getData()[0];

        $this->assertSame($query, $statement->getQuery());

        $result = $statement->execute(['domain' => $data[0]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareInvalidQuery()
    {
        $this->expectException(PostgresQueryError::class);
        $this->expectExceptionMessage('column "invalid" does not exist');

        $query = "SELECT * FROM test WHERE invalid=\$1";

        $statement = $this->executor->prepare($query);

        $statement->execute(['param']);
    }

    /**
     * @depends testPrepare
     */
    public function testPrepareSameQuery()
    {
        $sql = "SELECT * FROM test WHERE domain=\$1";

        $statement1 = $this->executor->prepare($sql);

        $statement2 = $this->executor->prepare($sql);

        $this->assertInstanceOf(SqlStatement::class, $statement1);
        $this->assertInstanceOf(SqlStatement::class, $statement2);

        unset($statement1);

        $data = $this->getData()[0];

        $result = $statement2->execute([$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testPrepareSameQuery
     */
    public function testSimultaneousPrepareSameQuery()
    {
        $sql = "SELECT * FROM test WHERE domain=\$1";

        $statement1 = async(fn () => $this->executor->prepare($sql));
        $statement2 = async(fn () => $this->executor->prepare($sql));

        [$statement1, $statement2] = Future\await([$statement1, $statement2]);

        $data = $this->getData()[0];

        $result = $statement1->execute([$data[0]]);

        $this->verifyResult($result, [$data]);

        unset($statement1);

        $result = $statement2->execute([$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    public function testPrepareSimilarQueryReturnsDifferentStatements()
    {
        $statement1 = async(fn () => $this->executor->prepare("SELECT * FROM test WHERE domain=\$1"));

        $statement2 = async(fn () => $this->executor->prepare("SELECT * FROM test WHERE domain=:domain"));

        [$statement1, $statement2] = Future\await([$statement1, $statement2]);

        $this->assertInstanceOf(SqlStatement::class, $statement1);
        $this->assertInstanceOf(SqlStatement::class, $statement2);

        $this->assertNotSame($statement1, $statement2);

        $data = $this->getData()[0];

        $results = [];

        $results[] = \iterator_to_array($statement1->execute([$data[0]]));
        $results[] = \iterator_to_array($statement2->execute(['domain' => $data[0]]));

        foreach ($results as $result) {
            /** @var SqlResult $result */
            foreach ($result as $row) {
                $this->assertSame($data[0], $row['domain']);
                $this->assertSame($data[1], $row['tld']);
            }
        }
    }

    public function testPrepareThenExecuteWithUnconsumedTupleResult()
    {
        $statement = $this->executor->prepare("SELECT * FROM test");

        $result = $statement->execute();

        unset($result); // Force destruction of result object.

        $result = $statement->execute();

        $data = $this->getData();

        $this->verifyResult($result, $data);
    }

    public function testExecute()
    {
        $data = $this->getData()[0];

        $result = $this->executor->execute("SELECT * FROM test WHERE domain=\$1", [$data[0]]);

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithNamedParams()
    {
        $data = $this->getData()[0];

        $result = $this->executor->execute(
            "SELECT * FROM test WHERE domain=:domain",
            ['domain' => $data[0]]
        );

        $this->verifyResult($result, [$data]);
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithInvalidParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Value for unnamed parameter at position 0 missing");

        $this->executor->execute("SELECT * FROM test WHERE domain=\$1");
    }

    /**
     * @depends testExecute
     */
    public function testExecuteWithInvalidNamedParams()
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Value for named parameter 'domain' missing");

        $this->executor->execute("SELECT * FROM test WHERE domain=:domain", ['tld' => 'com']);
    }

    /**
     * @depends testQueryWithTupleResult
     */
    public function testSimultaneousQuery()
    {
        $callback = fn (int $value) => async(function () use ($value): void {
            $result = $this->executor->query("SELECT {$value} as value");

            foreach ($result as $row) {
                $this->assertEquals($value, $row['value']);
            }
        });

        Future\await([$callback(0), $callback(1)]);
    }

    /**
     * @depends testSimultaneousQuery
     */
    public function testSimultaneousQueryWithOneFailing()
    {
        $callback = fn (string $query) => async(function () use ($query): SqlResult {
            $result = $this->executor->query($query);

            $data = $this->getData();

            $i = 0;
            foreach ($result as $row) {
                $this->assertSame($data[$i][0], $row['domain']);
                $this->assertSame($data[$i][1], $row['tld']);
                ++$i;
            }

            return $result;
        });

        $result = null;

        try {
            $successful = $callback("SELECT * FROM test");
            $failing = $callback("SELECT & FROM test");

            $result = $successful->await();
            $failing->await();
        } catch (SqlQueryError $exception) {
            $this->assertInstanceOf(SqlResult::class, $result);
            return;
        }

        $this->fail(\sprintf("Test did not throw an instance of %s", SqlQueryError::class));
    }

    public function testSimultaneousQueryAndPrepare()
    {
        $promises = [];
        $promises[] = async(function () {
            $result = $this->executor->query("SELECT * FROM test");
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        $promises[] = async(function () {
            $statement = ($this->executor->prepare("SELECT * FROM test"));
            $result = $statement->execute();
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        Future\await($promises);
    }

    public function testSimultaneousPrepareAndExecute()
    {
        $promises[] = async(function () {
            $statement = $this->executor->prepare("SELECT * FROM test");
            $result = $statement->execute();
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        $promises[] = async(function () {
            $result = $this->executor->execute("SELECT * FROM test");
            $data = $this->getData();
            $this->verifyResult($result, $data);
        });

        Future\await($promises);
    }

    public function testTransaction()
    {
        $transaction = $this->executor->beginTransaction();

        $this->assertInstanceOf(PostgresTransaction::class, $transaction);

        $data = $this->getData()[0];

        $this->assertFalse($transaction->isClosed());
        $this->assertTrue($transaction->isActive());

        $nested = $transaction->beginTransaction();

        $statement = $nested->prepare("SELECT * FROM test WHERE domain=:domain");
        $result = $statement->execute(['domain' => $data[0]]);

        unset($result, $statement); // Force destruction of result object.

        $result = $nested->execute("SELECT * FROM test WHERE domain=\$1 FOR UPDATE", [$data[0]]);

        unset($result); // Force destruction of result object.

        $nested->rollback();

        $transaction->commit();

        $this->assertTrue($transaction->isClosed());
        $this->assertFalse($transaction->isActive());

        try {
            $result = $transaction->execute("SELECT * FROM test");
            $this->fail('Query should fail after transaction commit');
        } catch (SqlTransactionError $exception) {
            // Exception expected.
        }
    }

    public function provideInsertParameters(): iterable
    {
        $data = \str_repeat("\0", 10);

        yield [
            "INSERT INTO test
                (domain, tld, keys, enabled, number, bytea)
                VALUES (:domain, :tld, :keys, :enabled, :number, :bytea)",
            "SELECT bytea FROM test WHERE domain = :domain",
            [
                'domain' => 'gitlab',
                'tld' => 'com',
                'keys' => [1, 2, 3],
                'enabled' => false,
                'number' => 2.718,
                'bytea' => new PostgresByteA($data),
            ],
            ['bytea' => $data],
        ];
    }

    public function testEscapeByteA(): void
    {
        $this->assertSame('\x00', $this->executor->escapeByteA("\0"));
    }

    public function testQuoteString(): void
    {
        $this->assertSame("'\"''test''\"'", $this->executor->quoteLiteral("\"'test'\""));
    }

    public function testQuoteName(): void
    {
        $this->assertSame("\"\"\"'test'\"\"\"", $this->executor->quoteIdentifier("\"'test'\""));
    }

    /**
     * @dataProvider provideInsertParameters
     */
    public function testStatementInsertByteA(
        string $insertSql,
        string $selectSql,
        array $params,
        array $expected
    ): void {
        $statement = $this->executor->prepare($insertSql);

        $result = $statement->execute($params);

        $this->assertSame(1, $result->getRowCount());

        $result = $this->executor->execute($selectSql, $params);
        $this->assertSame($expected, $result->fetchRow());
    }

    /**
     * @dataProvider provideInsertParameters
     */
    public function testExecuteInsertByteA(
        string $insertSql,
        string $selectSql,
        array $params,
        array $expected
    ): void {
        $result = $this->executor->execute($insertSql, $params);

        $this->assertSame(1, $result->getRowCount());

        $result = $this->executor->execute($selectSql, $params);
        $this->assertSame($expected, $result->fetchRow());
    }
}
