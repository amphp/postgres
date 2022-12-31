<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

/** @internal */
const STATEMENT_PARAM_REGEX = <<<'REGEX'
[
    # Skip all quoted groups.
    (['"])(?:\\(?:\\|\1)|(?!\1).)*+\1(*SKIP)(*FAIL)
    |
    # Unnamed parameters.
    (?<unnamed>
        \$(?<numbered>\d+)
        |
        # Match all question marks except those surrounded by "operator"-class characters on either side.
        (?<!(?<operators>[-+\\*/<>~!@#%^&|`?]))
        \?
        (?!\g<operators>|=)
        |
        :\?
    )
    |
    # Named parameters.
    (?<!:):(?<named>[a-zA-Z_][a-zA-Z0-9_]*)
]msxS
REGEX;

/**
 * @internal
 *
 * @param string $sql SQL statement with named and unnamed placeholders.
 * @param-out list<int|string> $names Array of parameter positions mapped to names and/or indexed locations.
 *
 * @return string SQL statement with Postgres-style placeholders
 */
function parseNamedParams(string $sql, ?array &$names): string
{
    $names = [];
    return \preg_replace_callback(STATEMENT_PARAM_REGEX, function (array $matches) use (&$names): string {
        static $index = 0, $unnamed = 0, $numbered = 1;

        if (isset($matches['named'])) {
            $names[$index] = $matches['named'];
        } elseif (!isset($matches['numbered'])) {
            $names[$index] = $unnamed++;
        } else {
            if ($unnamed > 0) {
                throw new \Error("Cannot mix unnamed (? placeholders) with numbered parameters");
            }

            $position = (int) $matches['numbered'];
            if ($position <= 0 || $position > $numbered + 1) {
                throw new \Error("Numbered placeholders must be sequential starting at 1");
            }

            $numbered = \max($position, $numbered);
            $names[$index] = $position - 1;
        }

        return '$' . ++$index;
    }, $sql);
}

/**
 * @internal
 *
 * @param array $params User-provided array of statement parameters.
 * @param list<int|string> $names Array generated by the $names param of {@see parseNamedParams()}.
 *
 * @return list<int|float|string|null>
 *
 * @throws \Error If the $param array does not contain a key corresponding to a named parameter.
 */
function replaceNamedParams(array $params, array $names): array
{
    $values = [];
    foreach ($names as $index => $name) {
        if (!\array_key_exists($name, $params)) {
            if (\is_int($name)) {
                $message = \sprintf("Value for unnamed parameter at position %s missing", $name);
            } else {
                $message = \sprintf("Value for named parameter '%s' missing", $name);
            }

            throw new \Error($message);
        }

        $values[] = cast($params[$name]);
    }

    return $values;
}

/**
 * @internal
 *
 * Casts a PHP value to a representation that is understood by Postgres, including encoding arrays.
 *
 * @throws \Error If $value is an object which is not a BackedEnum or Stringable, a resource, or an unknown type.
 */
function cast(mixed $value): string|int|float|null
{
    return match (\gettype($value)) {
        "NULL", "integer", "double", "string" => $value,
        "boolean" => $value ? 't' : 'f',
        "array" => '{' . \implode(',', \array_map(encodeArrayItem(...), $value)) . '}',
        "object" => match (true) {
            $value instanceof \BackedEnum => $value->value,
            $value instanceof \Stringable => (string) $value,
            default => throw new \ValueError(
                "An object in parameter values must be a BackedEnum or implement Stringable; got instance of "
                . \get_debug_type($value)
            ),
        },
        default => throw new \ValueError(\sprintf(
            "Invalid value type '%s' in parameter values",
            \get_debug_type($value),
        )),
    };
}

/**
 * @internal
 *
 * Wraps string in double-quotes for inclusion in an array.
 */
function encodeArrayItem(mixed $value): mixed
{
    return match (\gettype($value)) {
        "NULL" => "NULL",
        "string" => '"' . \str_replace(['\\', '"'], ['\\\\', '\\"'], $value) . '"',
        "object" => match (true) {
            $value instanceof \BackedEnum => encodeArrayItem($value->value),
            $value instanceof \Stringable => encodeArrayItem((string) $value),
            default => throw new \ValueError(
                "An object in parameter arrays must be a BackedEnum or implement Stringable; "
                . "got instance of " . \get_debug_type($value)
            ),
        },
        default => cast($value),
    };
}
