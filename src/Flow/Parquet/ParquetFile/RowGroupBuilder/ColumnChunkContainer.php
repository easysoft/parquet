<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

final class ColumnChunkContainer
{
    /**
     * @readonly
     * @var string
     */
    public $binaryBuffer;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\RowGroup\ColumnChunk
     */
    public $columnChunk;
    public function __construct(string $binaryBuffer, ColumnChunk $columnChunk)
    {
        $this->binaryBuffer = $binaryBuffer;
        $this->columnChunk = $columnChunk;
    }
}
