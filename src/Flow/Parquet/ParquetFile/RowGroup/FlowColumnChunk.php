<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

final class FlowColumnChunk
{
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\RowGroup\ColumnChunk
     */
    public $chunk;
    /**
     * @readonly
     * @var int
     */
    public $rowsOffset;
    /**
     * @readonly
     * @var int
     */
    public $rowsInGroup;
    public function __construct(ColumnChunk $chunk, int $rowsOffset, int $rowsInGroup)
    {
        $this->chunk = $chunk;
        $this->rowsOffset = $rowsOffset;
        $this->rowsInGroup = $rowsInGroup;
    }
}
