<?php
declare(strict_types=1);

namespace Amp\Postgres\Test;

use PHPUnit\Framework\TestCase;
use function Amp\Postgres\Internal\parseNamedParams;

final class InternalFunctionsTest extends TestCase
{
    /**
     * Tests that unnamed parameters are substituted correctly.
     *
     * @see parseNamedParams
     * @dataProvider provideUnnamedParams
     */
    public function testParseUnnamedParams(string $sqlIn, string $sqlOut): void
    {
        self::assertSame($sqlOut, parseNamedParams($sqlIn, $dummy));
    }

    public function provideUnnamedParams(): iterable
    {
        return [
            'Bare' => ['SELECT ?', 'SELECT $1'],
            'Parenthesized' => ['SELECT (?)', 'SELECT ($1)'],
            'Row constructor' => ['SELECT (?, ?)', 'SELECT ($1, $2)'],
            // Special-case exclude the =? operator to permit the following usage.
            '=? operator' => ['UPDATE foo SET bar=?', 'UPDATE foo SET bar=$1'],
            '= ? operator' => ['UPDATE foo SET bar = ?', 'UPDATE foo SET bar = $1'],
        ];
    }

    /**
     * Tests that operators containing the question mark character, that could be confused with unnamed parameters,
     * are parsed correctly.
     *
     * @see parseNamedParams
     * @see https://github.com/amphp/postgres/issues/39
     * @dataProvider provideProblematicOperators
     */
    public function testParseProblematicOperators(string $sql): void
    {
        self::assertSame($sql, parseNamedParams($sql, $dummy));
    }

    public function provideProblematicOperators(): iterable
    {
        return [
            // JSONB operators. https://postgresql.org/docs/12/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
            // Bare question mark currently unsupported. ?| can be used instead.
            #'?' => ["SELECT WHERE '{\"foo\":null}'::jsonb ? 'foo'"],
            '?|' => ["SELECT WHERE '{\"foo\":null}'::jsonb ?| array['foo']"],
            '?&' => ["SELECT WHERE '{\"foo\":null}'::jsonb ?& array['foo']"],
            '@?' => ["SELECT WHERE '{\"foo\":1}'::jsonb @? '$ ? (@.foo > 0)'"],

            // Geometric operators. https://postgresql.org/docs/12/functions-geometry.html#FUNCTIONS-GEOMETRY-OP-TABLE
            '?- unary' => ["SELECT WHERE ?- lseg '((-1,0),(1,0))'"],
            '?-' => ["SELECT WHERE point '(1,0)' ?- point '(0,0)'"],
            '?-|' => ["SELECT WHERE lseg '((0,0),(0,1))' ?-| lseg '((0,0),(1,0))'"],
            '?||' => ["SELECT WHERE lseg '((-1,0),(1,0))' ?|| lseg '((-1,2),(1,2))'"],
        ];
    }

    /**
     * @dataProvider provideRepeatedNumberedParams
     */
    public function testRepeatedNumberedParams(string $sql, array $expectedNames): void
    {
        parseNamedParams($sql, $names);
        self::assertSame($expectedNames, $names);
    }

    public function provideRepeatedNumberedParams(): iterable
    {
        return [
            ['SELECT * FROM table WHERE x=$1 AND y=$1', [0, 0]],
            ['SELECT * FROM table WHERE x=$1 AND y=$2 AND z=$1', [0, 1, 0]],
            ['SELECT * FROM table WHERE x=$1 AND y=$1 AND z=$2', [0, 0, 1]],
            ['SELECT * FROM table WHERE x=$1 AND y=:y AND z=$1', [0, 'y', 0]],
        ];
    }
}
