<?php

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\Sql\Common\RetrySqlConnector;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;
use Revolt\EventLoop;

/**
 * @param SqlConnector<PostgresConfig, PostgresConnection>|null $connector
 *
 * @return SqlConnector<PostgresConfig, PostgresConnection>
 */
function postgresConnector(?SqlConnector $connector = null): SqlConnector
{
    static $map;
    $map ??= new \WeakMap();
    $driver = EventLoop::getDriver();

    if ($connector) {
        return $map[$driver] = $connector;
    }

    return $map[$driver] ??= new RetrySqlConnector(new DefaultPostgresConnector());
}

/**
 * Create a connection using the global Connector instance.
 *
 * @throws SqlException If connecting fails.
 *
 * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
 */
function connect(PostgresConfig $config, ?Cancellation $cancellation = null): PostgresConnection
{
    return postgresConnector()->connect($config, $cancellation);
}

/**
 * Casts a PHP value to a representation that is understood by Postgres, including encoding arrays.
 *
 * @throws \Error If $value is an object without a __toString() method, a resource, or an unknown type.
 */
function cast(mixed $value): string|int|float|null
{
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
 * @return string The serialized representation of the array.
 *
 * @throws \Error If $array contains an object without a __toString() method, a resource, or an unknown type.
 */
function encode(array $array): string
{
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
