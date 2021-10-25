<?php

namespace Amp\Postgres;

use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\FailureException;
use Revolt\EventLoop;

function connector(?Connector $connector = null): Connector
{
    static $map;
    $map ??= new \WeakMap();
    $driver = EventLoop::getDriver();

    if ($connector) {
        return $map[$driver] = $connector;
    }

    return $map[$driver] ??= new TimeoutConnector;
}

/**
 * Create a connection using the global Connector instance.
 *
 * @param SqlConnectionConfig $config
 *
 * @return Connection
 *
 * @throws FailureException If connecting fails.
 *
 * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
 *
 * @codeCoverageIgnore
 */
function connect(SqlConnectionConfig $config): Connection
{
    return connector()->connect($config);
}

/**
 * Create a pool using the global Connector instance.
 *
 * @param SqlConnectionConfig $config
 * @param int                 $maxConnections
 * @param int                 $idleTimeout
 * @param bool                $resetConnections
 *
 * @return Pool
 */
function pool(
    SqlConnectionConfig $config,
    int $maxConnections = ConnectionPool::DEFAULT_MAX_CONNECTIONS,
    int $idleTimeout = ConnectionPool::DEFAULT_IDLE_TIMEOUT,
    bool $resetConnections = true
): Pool {
    return new Pool($config, $maxConnections, $idleTimeout, $resetConnections, connector());
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
 * @param array $array
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
