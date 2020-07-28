<p align="center">
<a href="https://amphp.org/postgres"><img src="https://raw.githubusercontent.com/amphp/logo/master/repos/postgres.png?v=12-07-2017" alt="postgres"/></a>
</p>

<p align="center">
<a href="https://travis-ci.org/amphp/postgres"><img src="https://img.shields.io/travis/amphp/postgres/master.svg?style=flat-square" alt="Build Status"/></a>
<a href="https://coveralls.io/github/amphp/postgres?branch=master"><img src="https://img.shields.io/coveralls/amphp/postgres/master.svg?style=flat-square" alt="Code Coverage"/></a>
<a href="https://github.com/amphp/postgres/releases"><img src="https://img.shields.io/github/release/amphp/postgres.svg?style=flat-square" alt="Release"/></a>
<a href="https://github.com/amphp/postgres/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License"/></a>
</p>

<p align="center"><strong>Async PostgreSQL client built with <a href="https://amphp.org/">Amp</a>.</strong></p>

## Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require amphp/postgres
```

## Requirements

- PHP 7.0+
- [ext-pgsql](https://secure.php.net/pgsql) or [pecl-pq](https://pecl.php.net/package/pq)

Note: [pecl-ev](https://pecl.php.net/package/ev) is not compatible with ext-pgsql. If you wish to use pecl-ev for the event loop backend, you must use pecl-pq.

## Documentation & Examples

Prepared statements and parameterized queries support named placeholders, as well as `?` and standard numeric (i.e. `$1`) placeholders.

More examples can be found in the [`examples`](examples) directory.

```php
use Amp\Postgres;
use Amp\Postgres\ConnectionConfig;
use Amp\Sql\Statement;

Amp\Loop::run(function () {
    $config = ConnectionConfig::fromString("host=localhost user=postgres db=test");

    /** @var Postgres\Pool $pool */
    $pool = Postgres\pool($config);

    /** @var Statement $statement */
    $statement = yield $pool->prepare("SELECT * FROM test WHERE id = :id");

    /** @var Postgres\ResultSet $result */
    $result = yield $statement->execute(['id' => 1337]);
    while (yield $result->advance()) {
        $row = $result->getCurrent();
        // $row is an array (map) of column values. e.g.: $row['column_name']
    }
});
```

## Versioning

`amphp/postgres` follows the [semver](http://semver.org/) semantic versioning specification like all other `amphp` packages.

## Security

If you discover any security related issues, please email [`contact@amphp.org`](mailto:contact@amphp.org) instead of using the issue tracker.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
