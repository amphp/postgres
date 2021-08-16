<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\ParseException;

/**
 * @internal
 */
final class ArrayParser
{
    /**
     * @param string $data String representation of PostgreSQL array.
     * @param callable $cast Callback to cast parsed values.
     * @param string $delimiter Delimiter used to separate values.
     *
     * @return array Parsed column data.
     *
     * @throws ParseException
     */
    public function parse(string $data, callable $cast, string $delimiter = ','): array
    {
        $data = \trim($data);

        $parser = $this->parser($data, $cast, $delimiter);
        $data = \iterator_to_array($parser);

        if ($parser->getReturn() !== '') {
            throw new ParseException("Data left in buffer after parsing");
        }

        return $data;
    }

    /**
     * Recursive generator parser yielding array values.
     *
     * @param string $data Remaining buffer data.
     * @param callable $cast Callback to cast parsed values.
     * @param string $delimiter Delimiter used to separate values.
     *
     * @return \Generator
     *
     * @throws ParseException
     */
    private function parser(string $data, callable $cast, string $delimiter = ','): \Generator
    {
        if ($data === '') {
            throw new ParseException("Unexpected end of data");
        }

        if ($data[0] !== '{') {
            throw new ParseException("Missing opening bracket");
        }

        $data = \ltrim(\substr($data, 1));

        do {
            if ($data === '') {
                throw new ParseException("Unexpected end of data");
            }

            if ($data[0] === '}') { // Empty array
                return \ltrim(\substr($data, 1));
            }

            if ($data[0] === '{') { // Array
                $parser = $this->parser($data, $cast, $delimiter);
                yield \iterator_to_array($parser);
                $data = $parser->getReturn();
                $end = $this->trim($data, 0, $delimiter);
                continue;
            }

            if ($data[0] === '"') { // Quoted value
                for ($position = 1; isset($data[$position]); ++$position) {
                    if ($data[$position] === '\\') {
                        ++$position; // Skip next character
                        continue;
                    }

                    if ($data[$position] === '"') {
                        break;
                    }
                }

                if (!isset($data[$position])) {
                    throw new ParseException("Could not find matching quote in quoted value");
                }

                $yield = \stripslashes(\substr($data, 1, $position - 1));

                $end = $this->trim($data, $position + 1, $delimiter);
            } else { // Unquoted value
                $position = 0;
                while (isset($data[$position]) && $data[$position] !== $delimiter && $data[$position] !== '}') {
                    ++$position;
                }

                $yield = \trim(\substr($data, 0, $position));

                $end = $this->trim($data, $position, $delimiter);

                if (\strcasecmp($yield, "NULL") === 0) { // Literal NULL is always unquoted.
                    yield null;
                    continue;
                }
            }

            yield $cast($yield);
        } while ($end !== '}');

        return $data;
    }

    /**
     * @param string $data Data trimmed past next delimiter and any whitespace to the right of the delimiter.
     * @param int $position Position to start search for delimiter.
     * @param string $delimiter Delimiter used to separate values.
     *
     * @return string First non-whitespace character after given position.
     *
     * @throws ParseException
     */
    private function trim(string &$data, int $position, string $delimiter): string
    {
        $data = \ltrim(\substr($data, $position));

        if ($data === '') {
            throw new ParseException("Unexpected end of data");
        }

        $end = $data[0];

        if ($end !== $delimiter && $end !== '}') {
            throw new ParseException("Invalid delimiter");
        }

        $data = \ltrim(\substr($data, 1));

        return $end;
    }
}
