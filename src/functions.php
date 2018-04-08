<?php

namespace Amp\Postgres;

use Amp\Loop;
use Amp\Promise;

const LOOP_CONNECTOR_IDENTIFIER = Connector::class;

function connector(Connector $connector = null): Connector {
    if ($connector === null) {
        $connector = Loop::getState(LOOP_CONNECTOR_IDENTIFIER);
        if ($connector) {
            return $connector;
        }

        $connector = new TimeoutConnector;
    }

    Loop::setState(LOOP_CONNECTOR_IDENTIFIER, $connector);
    return $connector;
}

/**
 * Create a connection using the global Connector instance.
 *
 * @param string $connectionString
 *
 * @return \Amp\Promise<\Amp\Postgres\Connection>
 *
 * @throws \Amp\Postgres\FailureException If connecting fails.
 *
 * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
 *
 * @codeCoverageIgnore
 */
function connect(string $connectionString): Promise {
    return connector()->connect($connectionString);
}

/**
 * Create a pool using the global Connector instance.
 *
 * @param string $connectionString
 * @param int $maxConnections
 *
 * @return \Amp\Postgres\Pool
 */
function pool(string $connectionString, int $maxConnections = Pool::DEFAULT_MAX_CONNECTIONS): Pool {
    return new Pool($connectionString, $maxConnections, connector());
}

/**
 * Casts a PHP value to a representation that is understood by Postgres, including encoding arrays.
 *
 * @param mixed $value
 *
 * @return string|int|float|null
 *
 * @throws \Error If $value is an object without a __toString() method, a resource, or an unknown type.
 */
function cast($value) {
    switch ($type = \gettype($value)) {
        case "NULL":
        case "integer":
        case "double":
        case "string":
            return $value; // No casting necessary for numerics, strings, and null.

        case "boolean":
            return $value ? 't' : 'f';

        case "array":
            return encode($value);

        case "object":
            if (!\method_exists($value, "__toString")) {
                throw new \Error("Object without a __toString() method included in parameter values");
            }

            return (string) $value;

        default:
            throw new \Error("Invalid value type '$type' in parameter values");
    }
}

/**
 * Encodes an array into a PostgreSQL representation of the array.
 *
 * @param array $array
 *
 * @return string The serialized representation of the array.
 *
 * @throws \Error If $array contains an object without a __toString() method, a resource, or an unknown type.
 */
function encode(array $array): string {
    $array = \array_map(function ($value) {
        switch (\gettype($value)) {
            case "NULL":
                return "NULL";

            case "object":
                if (!\method_exists($value, "__toString")) {
                    throw new \Error("Object without a __toString() method in array");
                }

                $value = (string) $value;
                // no break

            case "string":
                return '"' . \str_replace(['\\', '"'], ['\\\\', '\\"'], $value) . '"';

            default:
                return cast($value); // Recursively encodes arrays and errors on invalid values.
        }
    }, $array);

    return '{' . \implode(',', $array) . '}';
}
