<?php

namespace Amp\Postgres;

use Amp\Promise;

final class PooledResultSet implements ResultSet
{
    /** @var ResultSet */
    private $result;

    /** @var callable|null */
    private $release;

    public function __construct(ResultSet $result, callable $release)
    {
        $this->result = $result;
        $this->release = $release;
    }

    public function __destruct()
    {
        ($this->release)();
    }

    public function advance(int $type = self::FETCH_ASSOC): Promise
    {
        return $this->result->advance($type);
    }

    public function getCurrent()
    {
        return $this->result->getCurrent();
    }

    public function numFields(): int
    {
        return $this->result->numFields();
    }
}
