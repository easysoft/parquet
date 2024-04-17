<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

class PhysicalType
{
    public const BOOLEAN = 0;
    public const BYTE_ARRAY = 6;
    public const DOUBLE = 5;
    public const FIXED_LEN_BYTE_ARRAY = 7;
    public const FLOAT = 4;
    public const INT32 = 1;
    public const INT64 = 2;
    public const INT96 = 3;
    // deprecated, only used by legacy implementations.
}
