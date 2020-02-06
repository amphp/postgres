<?php

namespace Amp\Postgres\Test;

use PHPUnit\Framework\TestCase;
use function Amp\Postgres\encode;

class EncodeTest extends TestCase
{
    public function testSingleDimensionalStringArray(): void
    {
        $array = ["one", "two", "three"];
        $string = '{"one","two","three"}';

        $this->assertSame($string, encode($array));
    }

    public function testMultiDimensionalStringArray(): void
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one","two",{"three","four"},"five"}';

        $this->assertSame($string, encode($array));
    }

    public function testQuotedStrings(): void
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one","two",{"three","four"},"five"}';

        $this->assertSame($string, encode($array));
    }

    public function testEscapedQuoteDelimiter(): void
    {
        $array = ['va"lue1', 'value"2'];
        $string = '{"va\\"lue1","value\\"2"}';

        $this->assertSame($string, encode($array));
    }

    public function testNullValue(): void
    {
        $array = ["one", null, "three"];
        $string = '{"one",NULL,"three"}';

        $this->assertSame($string, encode($array));
    }

    public function testSingleDimensionalIntegerArray(): void
    {
        $array = [1, 2, 3];
        $string = '{' . \implode(',', $array) . '}';

        $this->assertSame($string, encode($array));
    }

    public function testIntegerArrayWithNull(): void
    {
        $array = [1, 2, null, 3];
        $string = '{1,2,NULL,3}';

        $this->assertSame($string, encode($array));
    }

    public function testMultidimensionalIntegerArray(): void
    {
        $array = [1, 2, [3, 4], [5], 6, 7, [[8, 9], 10]];
        $string = '{1,2,{3,4},{5},6,7,{{8,9},10}}';

        $this->assertSame($string, encode($array));
    }

    public function testEscapedBackslashesInQuotedValue(): void
    {
        $array = ["test\\ing", "esca\\ped\\"];
        $string = '{"test\\\\ing","esca\\\\ped\\\\"}';

        $this->assertSame($string, encode($array));
    }

    public function testObjectWithoutToStringMethod(): void
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Object without a __toString() method in array');

        encode([new \stdClass]);
    }
}
