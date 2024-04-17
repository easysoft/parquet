<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

class Repetition
{
    public const OPTIONAL = 1;
    public const REPEATED = 2;
    public const REQUIRED = 0;
}
