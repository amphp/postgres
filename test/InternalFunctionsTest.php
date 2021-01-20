<?php
declare(strict_types=1);

namespace Amp\Postgres\Test;

use PHPUnit\Framework\TestCase;
use function Amp\Postgres\Internal\parseNamedParams;

final class InternalFunctionsTest extends TestCase
{
    /**
     * Tests that the JSONB `@?` operator is not substituted by the parameter parser.
     *
     * @see parseNamedParams
     * @see https://github.com/amphp/postgres/issues/39
     */
    public function testParseJsonbOperator(): void
    {
        self::assertSame(
            $sql = "SELECT * FROM foo WHERE bar::jsonb @? '$[*] ? (@.baz > 0)'",
            parseNamedParams($sql, $names)
        );
    }
}
