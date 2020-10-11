<?php

namespace Amp\Postgres;

interface Handle extends Receiver, Quoter
{
    public const STATEMENT_NAME_PREFIX = "amp_";
}
