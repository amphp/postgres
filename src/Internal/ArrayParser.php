<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Postgres\PostgresParseException;

/**
 * @internal
 */
final class ArrayParser
{
    use ForbidCloning;
    use ForbidSerialization;

    private const WHITESPACE_CHARS = " \n\r\t\v\0";

    /**
     * @param string $data String representation of PostgresSQL array.
     * @param \Closure(string):mixed $cast Callback to cast parsed values.
     * @param string $delimiter Delimiter used to separate values.
     *
     * @return list<mixed> Parsed column data.
     *
     * @throws PostgresParseException
     */
    public static function parse(string $data, \Closure $cast, string $delimiter = ','): array
    {
        $parser = new self($data, $cast, $delimiter);
        $result = $parser->parseToArray();

        if (isset($parser->data[$parser->position])) {
            throw new PostgresParseException("Data left in buffer after parsing");
        }

        return $result;
    }

    /**
     * @param string $data String representation of PostgresSQL array.
     * @param \Closure(string):mixed $cast Callback to cast parsed values.
     * @param string $delimiter Delimiter used to separate values.
     */
    private function __construct(
        private readonly string $data,
        private readonly \Closure $cast,
        private readonly string $delimiter,
        private int $position = 0,
    ) {
    }

    /**
     * @return list<mixed> Parsed column data.
     *
     * @throws PostgresParseException
     */
    private function parseToArray(): array
    {
        $result = [];

        $this->position = $this->skipWhitespace($this->position);

        if (!isset($this->data[$this->position])) {
            throw new PostgresParseException("Unexpected end of data");
        }

        if ($this->data[$this->position] !== '{') {
            throw new PostgresParseException("Missing opening bracket");
        }

        $this->position = $this->skipWhitespace($this->position + 1);

        do {
            if (!isset($this->data[$this->position])) {
                throw new PostgresParseException("Unexpected end of data");
            }

            if ($this->data[$this->position] === '}') { // Empty array
                $this->position = $this->skipWhitespace($this->position + 1);
                break;
            }

            if ($this->data[$this->position] === '{') { // Array
                $parser = new self($this->data, $this->cast, $this->delimiter, $this->position);
                $result[] = $parser->parseToArray();
                $this->position = $parser->position;
                $delimiter = $this->moveToNextDelimiter($this->position);
                continue;
            }

            if ($this->data[$this->position] === '"') { // Quoted value
                ++$this->position;
                for ($position = $this->position; isset($this->data[$position]); ++$position) {
                    if ($this->data[$position] === '\\') {
                        ++$position; // Skip next character
                        continue;
                    }

                    if ($this->data[$position] === '"') {
                        break;
                    }
                }

                if (!isset($this->data[$position])) {
                    throw new PostgresParseException("Could not find matching quote in quoted value");
                }

                $entry = \stripslashes(\substr($this->data, $this->position, $position - $this->position));

                $delimiter = $this->moveToNextDelimiter($position + 1);
            } else { // Unquoted value
                $position = $this->position;
                while (isset($this->data[$position])
                    && $this->data[$position] !== $this->delimiter
                    && $this->data[$position] !== '}'
                ) {
                    ++$position;
                }

                $entry = \trim(\substr($this->data, $this->position, $position - $this->position));

                $delimiter = $this->moveToNextDelimiter($position);

                if (\strcasecmp($entry, "NULL") === 0) { // Literal NULL is always unquoted.
                    $result[] = null;
                    continue;
                }
            }

            $result[] = ($this->cast)($entry);
        } while ($delimiter !== '}');

        return $result;
    }

    /**
     * @param int $position Position to start search for delimiter.
     *
     * @return string First non-whitespace character after given position.
     *
     * @throws PostgresParseException
     */
    private function moveToNextDelimiter(int $position): string
    {
        $position = $this->skipWhitespace($position);

        if (!isset($this->data[$position])) {
            throw new PostgresParseException("Unexpected end of data");
        }

        $delimiter = $this->data[$position];

        if ($delimiter !== $this->delimiter && $delimiter !== '}') {
            throw new PostgresParseException("Invalid delimiter");
        }

        $this->position = $this->skipWhitespace($position + 1);

        return $delimiter;
    }

    private function skipWhitespace(int $position): int
    {
        while (isset($this->data[$position]) && \str_contains(self::WHITESPACE_CHARS, $this->data[$position])) {
            ++$position;
        }

        return $position;
    }
}
