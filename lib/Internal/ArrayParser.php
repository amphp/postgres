<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\ParseException;

class ArrayParser {
    /**
     * @param string $data
     * @param callable|null $cast
     * @param string $delimiter
     *
     * @return array Parsed column data.
     *
     * @throws \Amp\Postgres\ParseException
     */
    public function parse(string $data, callable $cast = null, string $delimiter = ','): array {
        $data = \trim($data);

        if ($data[0] !== '{' || \substr($data, -1) !== '}') {
            throw new ParseException("Missing opening or closing brackets");
        }

        $parser = $this->parser($data, $cast, $delimiter);
        $data = \iterator_to_array($parser);

        if ($parser->getReturn() !== '') {
            throw new ParseException("Data left in buffer after parsing");
        }

        return $data;
    }

    private function parser(string $data, callable $cast = null, string $delimiter = ','): \Generator {
        $data = \ltrim(\substr($data, 1));

        do {
            if ($data === '') {
                throw new ParseException("Missing closing bracket");
            }

            if ($data[0] === '{') { // Array
                $parser = $this->parser($data, $cast, $delimiter);
                yield \iterator_to_array($parser);
                list($data, $end) = $this->trim($parser->getReturn(), 0, $delimiter);
                continue;
            }

            if ($data[0] === '"') { // Quoted value
                $position = 1;
                do {
                    $position = \strpos($data, '"', $position);
                    if ($position === false) {
                        throw new ParseException("Could not find matching quote in quoted value");
                    }
                } while ($data[$position - 1] === '\\' && ++$position); // Check for escaped "

                $yield = \str_replace('\\"', '"', \substr($data, 1, $position - 1));

                list($data, $end) = $this->trim($data, $position + 1, $delimiter);
            } else { // Unquoted value
                $position = 0;
                while (isset($data[$position]) && $data[$position] !== $delimiter && $data[$position] !== '}') {
                    ++$position;
                }

                $yield = \trim(\substr($data, 0, $position));

                list($data, $end) = $this->trim($data, $position, $delimiter);

                if (\strcasecmp($yield, "NULL") === 0) { // Literal NULL is always unquoted.
                    yield null;
                    continue;
                }
            }

            yield $cast ? $cast($yield) : $yield;
        } while ($end !== '}');

        return $data;
    }

    private function trim(string $data, int $position, string $delimiter): array {
        do {
            if (!isset($data[$position])) {
                throw new ParseException("Unexpected end of data");
            }

            $end = $data[$position];
        } while (\ltrim($end) === '' && isset($data[++$position]));

        if ($end !== $delimiter && $end !== '}') {
            throw new ParseException("Invalid delimiter");
        }

        $data = \ltrim(\substr($data, $position + 1));

        return [$data, $end];
    }
}
