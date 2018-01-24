<?php

namespace Amp\Postgres\Test;

use Amp\PHPUnit\TestCase;
use Amp\Postgres\Internal\ArrayParser;

class ArrayParserTest extends TestCase {
    /** @var \Amp\Postgres\Internal\ArrayParser */
    private $parser;

    public function setUp() {
        $this->parser = new ArrayParser;
    }

    public function testSingleDimensionalArray() {
        $array = ["one", "two", "three"];
        $string = '{' . \implode(',', $array) . '}';

        $this->assertSame($array, $this->parser->parse($string));
    }

    public function testMultiDimensionalArray() {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{one, two, {three, four}, five}';

        $this->assertSame($array, $this->parser->parse($string));
    }

    public function testQuotedStrings() {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one", "two", {"three", "four"}, "five"}';

        $this->assertSame($array, $this->parser->parse($string));
    }

    public function testAlternateDelimiter() {
        $array = ["1,2,3", "3,4,5"];
        $string = '{1,2,3;3,4,5}';

        $this->assertSame($array, $this->parser->parse($string, null, ';'));
    }

    public function testEscapedQuoteDelimiter() {
        $array = ['va"lue1', 'value"2'];
        $string = '{"va\\"lue1", "value\\"2"}';

        $this->assertSame($array, $this->parser->parse($string, null, ','));
    }

    public function testNullValue() {
        $array = ["one", null, "three"];
        $string = '{one, NULL, three}';

        $this->assertSame($array, $this->parser->parse($string));
    }

    public function testQuotedNullValue() {
        $array = ["one", "NULL", "three"];
        $string = '{one, "NULL", three}';

        $this->assertSame($array, $this->parser->parse($string));
    }

    public function testCast() {
        $array = [1, 2, 3];
        $string = '{' . \implode(',', $array) . '}';

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testCastWithNull() {
        $array = [1, 2, null, 3];
        $string = '{1,2,NULL,3}';

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testCastWithMultidimensionalArray() {
        $array = [1, 2, [3, 4], [5], 6, 7, [[8, 9], 10]];
        $string = '{1,2,{3,4},{5},6,7,{{8,9},10}}';

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testRandomWhitespace() {
        $array = [1, 2, [3, 4], [5], 6, 7, [[8, 9], 10]];
        $string = " {1, 2, { 3 ,\r 4 },{ 5} \n\t ,6 , 7, { {8,\t 9}, 10} }  \n";

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testEscapedBackslashesInQuotedValue() {
        $array = ["test\\ing", "esca\\ped\\"];
        $string = '{"test\\\\ing", "esca\\\\ped\\\\"}';

        $this->assertSame($array, $this->parser->parse($string));
    }

    /**
     * @expectedException \Amp\Postgres\ParseException
     * @expectedExceptionMessage Missing opening or closing brackets
     */
    public function testNoClosingBracket() {
        $string = '{"one", "two"';
        $this->parser->parse($string);
    }

    /**
     * @expectedException \Amp\Postgres\ParseException
     * @expectedExceptionMessage Data left in buffer after parsing
     */
    public function testTrailingData() {
        $string = '{"one", "two"} data}';
        $this->parser->parse($string);
    }

    /**
     * @expectedException \Amp\Postgres\ParseException
     * @expectedExceptionMessage Could not find matching quote in quoted value
     */
    public function testMissingQuote() {
        $string = '{"one", "two}';
        $this->parser->parse($string);
    }

    /**
     * @expectedException \Amp\Postgres\ParseException
     * @expectedExceptionMessage Invalid delimiter
     */
    public function testInvalidDelimiter() {
        $string = '{"one"; "two"}';
        $this->parser->parse($string);
    }
}
