<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnChunkBuilder
{
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Schema\FlatColumn
     */
    private $column;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Compressions
     */
    private $compression;
    /**
     * @readonly
     * @var \Flow\Parquet\Data\DataConverter
     */
    private $dataConverter;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\RowGroupBuilder\PageSizeCalculator
     */
    private $calculator;
    /**
     * @readonly
     * @var \Flow\Parquet\Options
     */
    private $options;
    /**
     * @var mixed[]
     */
    private $rows = [];

    /**
     * @var \Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnChunkStatistics
     */
    private $statistics;

    public function __construct(
        FlatColumn $column,
        Compressions $compression,
        DataConverter $dataConverter,
        PageSizeCalculator $calculator,
        Options $options
    ) {
        $this->column = $column;
        $this->compression = $compression;
        $this->dataConverter = $dataConverter;
        $this->calculator = $calculator;
        $this->options = $options;
        $this->statistics = new ColumnChunkStatistics($this->column);
    }

    /**
     * @param mixed $row
     */
    public function addRow($row) : void
    {
        $this->statistics->add($row);
        $this->rows[] = $row;
    }

    public function flush(int $fileOffset) : ColumnChunkContainer
    {
        $pageContainers = (new PagesBuilder($this->dataConverter, $this->compression, $this->calculator, $this->options))
            ->build($this->column, $this->rows, $this->statistics);

        $statistics = (new StatisticsBuilder($this->dataConverter))->build($this->column, $this->statistics);

        $this->statistics->reset();

        return new ColumnChunkContainer(
            $pageContainers->buffer(),
            new ColumnChunk(
                $this->column->type(),
                $this->compression,
                $pageContainers->valuesCount(),
                $fileOffset,
                $this->column->path(),
                $pageContainers->encodings(),
                $pageContainers->compressedSize(),
                $pageContainers->uncompressedSize(),
                ($pageContainers->dictionaryPageContainer()) ? $fileOffset : null,
                ($pageContainers->dictionaryPageContainer()) ? $fileOffset + $pageContainers->dictionaryPageContainer()->totalCompressedSize() : $fileOffset,
                null,
                $statistics
            )
        );
    }

    public function statistics() : ColumnChunkStatistics
    {
        return $this->statistics;
    }
}
