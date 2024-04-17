<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

class ConvertedType
{
    public const BSON = 20;
    public const DATE = 6;
    public const DECIMAL = 5;
    public const ENUM = 4;
    public const INT_16 = 16;
    public const INT_32 = 17;
    public const INT_64 = 18;
    public const INT_8 = 15;
    public const INTERVAL = 21;
    public const JSON = 19;
    public const LIST = 3;
    public const MAP = 1;
    public const MAP_KEY_VALUE = 2;
    public const TIME_MICROS = 8;
    public const TIME_MILLIS = 7;
    public const TIMESTAMP_MICROS = 10;
    public const TIMESTAMP_MILLIS = 9;
    public const UINT_16 = 12;
    public const UINT_32 = 13;
    public const UINT_64 = 14;
    public const UINT_8 = 11;
    public const UTF8 = 0;
}
