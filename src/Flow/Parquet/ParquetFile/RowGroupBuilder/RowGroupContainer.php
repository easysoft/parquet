<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroup;

final class RowGroupContainer
{
    /**
     * @readonly
     * @var string
     */
    public $binaryBuffer;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\RowGroup
     */
    public $rowGroup;
    public function __construct(string $binaryBuffer, RowGroup $rowGroup)
    {
        $this->binaryBuffer = $binaryBuffer;
        $this->rowGroup = $rowGroup;
    }
}
