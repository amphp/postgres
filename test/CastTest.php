<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use PHPUnit\Framework\TestCase;
use function Amp\Postgres\Internal\cast;

enum IntegerEnum: int
{
    case One = 1;
    case Two = 2;
    case Three = 3;
}

enum StringEnum: string
{
    case One = 'one';
    case Two = 'two';
    case Three = 'three';
}

enum UnitEnum
{
    case Case;
}

class CastTest extends TestCase
{
    public function testSingleDimensionalStringArray(): void
    {
        $array = ["one", "two", "three"];
        $string = '{"one","two","three"}';

        $this->assertSame($string, cast($array));
    }

    public function testMultiDimensionalStringArray(): void
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one","two",{"three","four"},"five"}';

        $this->assertSame($string, cast($array));
    }

    public function testQuotedStrings(): void
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one","two",{"three","four"},"five"}';

        $this->assertSame($string, cast($array));
    }

    public function testEscapedQuoteDelimiter(): void
    {
        $array = ['va"lue1', 'value"2'];
        $string = '{"va\\"lue1","value\\"2"}';

        $this->assertSame($string, cast($array));
    }

    public function testNullValue(): void
    {
        $array = ["one", null, "three"];
        $string = '{"one",NULL,"three"}';

        $this->assertSame($string, cast($array));
    }

    public function testSingleDimensionalIntegerArray(): void
    {
        $array = [1, 2, 3];
        $string = '{' . \implode(',', $array) . '}';

        $this->assertSame($string, cast($array));
    }

    public function testIntegerArrayWithNull(): void
    {
        $array = [1, 2, null, 3];
        $string = '{1,2,NULL,3}';

        $this->assertSame($string, cast($array));
    }

    public function testMultidimensionalIntegerArray(): void
    {
        $array = [1, 2, [3, 4], [5], 6, 7, [[8, 9], 10]];
        $string = '{1,2,{3,4},{5},6,7,{{8,9},10}}';

        $this->assertSame($string, cast($array));
    }

    public function testEscapedBackslashesInQuotedValue(): void
    {
        $array = ["test\\ing", "esca\\ped\\"];
        $string = '{"test\\\\ing","esca\\\\ped\\\\"}';

        $this->assertSame($string, cast($array));
    }

    public function testBackedEnum(): void
    {
        $this->assertSame(3, cast(IntegerEnum::Three));
        $this->assertSame('three', cast(StringEnum::Three));
    }

    public function testBackedEnumInArray(): void
    {
        $array = [
            [IntegerEnum::One, IntegerEnum::Two, IntegerEnum::Three],
            [StringEnum::One, StringEnum::Two, StringEnum::Three],
        ];
        $string = '{{1,2,3},{"one","two","three"}}';

        $this->assertSame($string, cast($array));
    }

    public function testUnitEnum(): void
    {
        $this->expectException(\TypeError::class);
        $this->expectExceptionMessage('An object in parameter values must be');

        cast(UnitEnum::Case);
    }

    public function testUnitEnumInArray(): void
    {
        $this->expectException(\TypeError::class);
        $this->expectExceptionMessage('An object in parameter arrays must be');

        cast([UnitEnum::Case]);
    }

    public function testObjectWithoutToStringMethod(): void
    {
        $this->expectException(\TypeError::class);
        $this->expectExceptionMessage('An object in parameter values must be');

        cast(new \stdClass);
    }

    public function testObjectWithoutToStringMethodInArray(): void
    {
        $this->expectException(\TypeError::class);
        $this->expectExceptionMessage('An object in parameter arrays must be');

        cast([new \stdClass]);
    }
}
