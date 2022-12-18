<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

abstract class AbstractConnectionTest extends AbstractLinkTest
{
    public function testIsClosed()
    {
        $this->assertFalse($this->link->isClosed());
    }
}
