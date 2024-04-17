<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

class Compressions
{
    public const BROTLI = 4;
    public const GZIP = 2;
    public const LZ4 = 5;
    public const LZ4_RAW = 7;
    public const LZO = 3;
    public const SNAPPY = 1;
    public const UNCOMPRESSED = 0;
    public const ZSTD = 6;
}
