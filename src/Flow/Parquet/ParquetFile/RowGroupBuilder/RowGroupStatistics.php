<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

final class RowGroupStatistics
{
    /**
     * @var array<ColumnChunkStatistics>
     * @readonly
     */
    private $statistics;
    /**
     * @var int
     */
    private $rowsCount = 0;

    /**
     * @param array<ColumnChunkStatistics> $statistics
     */
    public function __construct(array $statistics)
    {
        $this->statistics = $statistics;
    }

    public static function fromBuilders(array $columnChunkBuilders) : self
    {
        return new self(
            \array_map(static function (ColumnChunkBuilder $columnChunkBuilder) {
                return $columnChunkBuilder->statistics();
            }, $columnChunkBuilders)
        );
    }

    public function addRow() : void
    {
        $this->rowsCount++;
    }

    public function rowsCount() : int
    {
        return $this->rowsCount;
    }

    public function uncompressedSize() : int
    {
        $total = 0;

        foreach ($this->statistics as $statistic) {
            $total += $statistic->uncompressedSize();
        }

        return $total;
    }

    public function valuesCount() : int
    {
        $total = 0;

        foreach ($this->statistics as $statistic) {
            $total += $statistic->valuesCount();
        }

        return $total;
    }
}
