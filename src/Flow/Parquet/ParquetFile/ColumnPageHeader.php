<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnPageHeader
{
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Schema\FlatColumn
     */
    public $column;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\RowGroup\ColumnChunk
     */
    public $columnChunk;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Page\PageHeader
     */
    public $pageHeader;
    public function __construct(FlatColumn $column, ColumnChunk $columnChunk, PageHeader $pageHeader)
    {
        $this->column = $column;
        $this->columnChunk = $columnChunk;
        $this->pageHeader = $pageHeader;
    }
}
