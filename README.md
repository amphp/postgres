# PostgreSQL Client for Amp

This library is a component for [Amp](https://github.com/amphp/amp) that provides an asynchronous client for PostgreSQL.

[![Build Status](https://img.shields.io/travis/amphp/postgres/master.svg?style=flat-square)](https://travis-ci.org/amphp/postgres)
[![Coverage Status](https://img.shields.io/coveralls/amphp/postgres/master.svg?style=flat-square)](https://coveralls.io/r/amphp/postgres)
[![Semantic Version](https://img.shields.io/github/release/amphp/postgres.svg?style=flat-square)](http://semver.org)
[![MIT License](https://img.shields.io/packagist/l/amphp/postgres.svg?style=flat-square)](LICENSE)
[![@amphp on Twitter](https://img.shields.io/badge/twitter-%40asyncphp-5189c7.svg?style=flat-square)](https://twitter.com/asyncphp)

##### Requirements

- PHP 7+
- [ext-pgsql](https://secure.php.net/pgsql) or [pecl-pq](http://pecl.php.net/package/pq)

##### Installation

The recommended way to install is with the [Composer](http://getcomposer.org/) package manager. (See the [Composer installation guide](https://getcomposer.org/doc/00-intro.md) for information on installing and using Composer.)

Run the following command to use this library in your project: 

```bash
composer require amphp/postgres
```

You can also manually edit `composer.json` to add this library as a project requirement.

```json
// composer.json
{
    "require": {
        "amphp/postgres": "^0.2"
    }
}
```

#### Example

```php
#!/usr/bin/env php
<?php

require __DIR__ . '/vendor/autoload.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    /** @var \Amp\Postgres\Connection $connection */
    $connection = yield Postgres\connect('host=localhost user=postgres dbname=test');

    /** @var \Amp\Postgres\Statement $statement */
    $statement = yield $connection->prepare('SELECT * FROM test WHERE id=$1');

    /** @var \Amp\Postgres\TupleResult $result */
    $result = yield $statement->execute(1337);

    while (yield $result->advance()) {
        $row = $result->getCurrent();
        // $row is an array (map) of column values. e.g.: $row['column_name']
    }
});
```
