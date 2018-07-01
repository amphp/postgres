<?php

namespace Amp\Postgres\Test;

use Amp\PHPUnit\TestCase;
use function Amp\Postgres\encode;

class EncodeTest extends TestCase
{
    public function testSingleDimensionalStringArray()
    {
        $array = ["one", "two", "three"];
        $string = '{"one","two","three"}';

        $this->assertSame($string, encode($array));
    }

    public function testMultiDimensionalStringArray()
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one","two",{"three","four"},"five"}';

        $this->assertSame($string, encode($array));
    }

    public function testQuotedStrings()
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one","two",{"three","four"},"five"}';

        $this->assertSame($string, encode($array));
    }

    public function testEscapedQuoteDelimiter()
    {
        $array = ['va"lue1', 'value"2'];
        $string = '{"va\\"lue1","value\\"2"}';

        $this->assertSame($string, encode($array));
    }

    public function testNullValue()
    {
        $array = ["one", null, "three"];
        $string = '{"one",NULL,"three"}';

        $this->assertSame($string, encode($array));
    }

    public function testSingleDimensionalIntegerArray()
    {
        $array = [1, 2, 3];
        $string = '{' . \implode(',', $array) . '}';

        $this->assertSame($string, encode($array));
    }

    public function testIntegerArrayWithNull()
    {
        $array = [1, 2, null, 3];
        $string = '{1,2,NULL,3}';

        $this->assertSame($string, encode($array));
    }

    public function testMultidimensionalIntegerArray()
    {
        $array = [1, 2, [3, 4], [5], 6, 7, [[8, 9], 10]];
        $string = '{1,2,{3,4},{5},6,7,{{8,9},10}}';

        $this->assertSame($string, encode($array));
    }

    public function testEscapedBackslashesInQuotedValue()
    {
        $array = ["test\\ing", "esca\\ped\\"];
        $string = '{"test\\\\ing","esca\\\\ped\\\\"}';

        $this->assertSame($string, encode($array));
    }

    /**
     * @expectedException \Error
     * @expectedExceptionMessage Object without a __toString() method in array
     */
    public function testObjectWithoutToStringMethod()
    {
        encode([new \stdClass]);
    }
}
