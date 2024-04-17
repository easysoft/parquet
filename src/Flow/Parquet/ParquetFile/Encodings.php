<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

class Encodings
{
    public const BIT_PACKED = 4;
    public const BYTE_STREAM_SPLIT = 9;
    public const DELTA_BINARY_PACKED = 5;
    public const DELTA_BYTE_ARRAY = 7;
    public const DELTA_LENGTH_BYTE_ARRAY = 6;
    public const PLAIN = 0;
    public const PLAIN_DICTIONARY = 2;
    public const RLE = 3;
    public const RLE_DICTIONARY = 8;
}
